package carbonserver

import (
	"errors"
	"fmt"
	"strings"
)

// value list query parser and rewriter

func (listener *CarbonserverListener) expandGlobBracesForTrieIndex(query string) ([]string, error) {
	vlq, err := parseValueLists(query)
	if err != nil {
		return nil, err
	}

	// TODO: test max glob failing logics

	// glob expansion limit is a good thing because it's trival for user or
	// tool to craft a query like {a,b,c,d}{a,b,c,d}{a,b,c,d}
	// {a,b,c,d}... and it can trigger oom in go-carbon because of the
	// exponential expansion result.
	limit := listener.maxGlobs
	subvlqs, err := vlq.expandBySubquery(limit)
	if err != nil && (!errors.Is(err, errMaxGlobsExhausted) || listener.failOnMaxGlobs) {
		return nil, err
	}

	var subqueries []string
	for _, svlq := range subvlqs {
		subqueries = append(subqueries, svlq.String())
	}

	return subqueries, nil
}

type vlQueryNode interface {
	vlQueryNode()
	clone() vlQueryNode
}

type vlQuery []vlQueryNode

func (vlQuery) vlQueryNode() {}

func (vlq *vlQuery) clone() vlQueryNode {
	var vlqc vlQuery
	for _, item := range *vlq {
		vlqc = append(vlqc, item.clone())
	}
	return &vlqc
}

// TODO: could just be new type to vlQuery?
type vlQueryList []vlQueryNode

func (vlQueryList) vlQueryNode() {}

func (vlql *vlQueryList) clone() vlQueryNode {
	var vlqlc vlQueryList
	for _, item := range *vlql {
		vlqlc = append(vlqlc, item.clone())
	}
	return &vlqlc
}

type vlQueryLiteral string

func (vlQueryLiteral) vlQueryNode() {}

func (vlql vlQueryLiteral) clone() vlQueryNode {
	return vlql
}

func (vlq *vlQuery) String() string {
	var qstr string
	for _, nx := range *vlq {
		switch n := nx.(type) {
		case *vlQuery:
			qstr += n.String()
		case *vlQueryLiteral:
			qstr += string(*n)
		case vlQueryLiteral:
			qstr += string(n)
		case *vlQueryList:
			var items []string
			for _, item := range *n {
				items = append(items, (&vlQuery{item}).String())
			}
			qstr += fmt.Sprintf("{%s}", strings.Join(items, ","))
		default:
			panic(fmt.Sprintf("unknown vlQueryNodeType %T", n))
		}
	}

	return qstr
}

// var errValueQueryListTooBig = errors.New("value list too big, please break down or rewrite the query")

// TODO: rate limit
func (vlq *vlQuery) expandBySubquery(limit int) ([]*vlQuery, error) {
	if limit <= 0 {
		return nil, errMaxGlobsExhausted
	}

	// flatten top sub queries
	for {
		var hasSubquery bool
		var newNodes = make([]vlQueryNode, 0, len(*vlq))
		for _, x := range *vlq {
			if node, ok := x.(*vlQuery); ok {
				newNodes = append(newNodes, *node...)
				hasSubquery = true
			} else {
				newNodes = append(newNodes, x)
			}
		}

		if !hasSubquery {
			break
		}

		*vlq = newNodes
	}

	for i, n := range *vlq {
		node, ok := n.(*vlQueryList)
		if !ok {
			continue
		}

		var listsNext = [][]*vlQueryList{{node}}
		var lindices = [][][]int{{{i}}}
		var depth int
		for len(listsNext[depth]) > 0 {
			depth++
			listsNext = append(listsNext, nil)
			lindices = append(lindices, nil)

			for lindex, list := range listsNext[depth-1] {
				var newList = [][]vlQueryNode{{}}
				var indices = []int{0}
				for j, item := range *list {
					switch inode := item.(type) {
					case *vlQuery:
						newList = append(newList, []vlQueryNode{item})
						indices = append(indices, j)
					case vlQueryLiteral:
						if strings.Contains(string(inode), ".") {
							newList = append(newList, []vlQueryNode{item})
							indices = append(indices, j)
						} else {
							newList[0] = append(newList[0], item)
						}
					case *vlQueryList:
						// trying to de-value-list items as deep as possible
						var sublists = []*vlQueryList{inode}
						for len(sublists) > 0 {
							var newSubLists []*vlQueryList
							for _, sl := range sublists {
								for _, sitem := range *sl {
									switch itemn := sitem.(type) {
									case *vlQueryList:
										newSubLists = append(newSubLists, itemn)
									case vlQueryLiteral:
										if strings.Contains(string(itemn), ".") {
											newList = append(newList, []vlQueryNode{sitem})
											indices = append(indices, j)
										} else {
											newList[0] = append(newList[0], sitem)
										}
									default:
										newList[0] = append(newList[0], sitem)
									}
								}
							}
							sublists = newSubLists
						}
					default:
						newList[0] = append(newList[0], item)
					}
				}

				// no split needed, skips
				if len(newList) == 1 {
					continue
				}

				var newQueries []*vlQuery
				if len(newList[0]) > 0 {
					newq := vlq.clone().(*vlQuery)
					indices := lindices[depth-1][lindex]
					if len(newList[0]) == 1 {
						newq.replaceNodeAt(indices, newList[0][0])
					} else {
						newq.replaceNodeAt(indices, (*vlQueryList)(&newList[0]))
					}
					expandedSubqueries, err := newq.expandBySubquery(limit - len(newQueries))
					newQueries = append(newQueries, expandedSubqueries...)
					if err != nil {
						return newQueries, err
					}
				}
				for i, newl := range newList {
					if i == 0 {
						continue
					}

					newq := vlq.clone().(*vlQuery)
					indices := lindices[depth-1][lindex]
					newq.replaceNodeAt(indices, newl[0])
					expandedSubqueries, err := newq.expandBySubquery(limit - len(newQueries))
					newQueries = append(newQueries, expandedSubqueries...)
					if err != nil {
						return newQueries, err
					}
				}

				return newQueries, nil
			}
		}
	}

	return []*vlQuery{vlq}, nil
}

func (q *vlQuery) replaceNodeAt(indices []int, newItems vlQueryNode) error {
	var idxp int
	var node vlQueryNode = q
	for idxp < len(indices) {
		switch x := node.(type) {
		case *vlQuery:
			if idxp == len(indices)-1 {
				(*x)[indices[idxp]] = newItems
				return nil
			}

			node = (*x)[indices[idxp]]
		case *vlQueryList:
			if idxp == len(indices)-1 {
				(*x)[indices[idxp]] = newItems
				return nil
			}

			node = (*x)[indices[idxp]]
		default:
			return fmt.Errorf("can't index non-indexable node: %T", node)
		}

		idxp++
	}

	return fmt.Errorf("failed to replace the node")
}

type vlQueryParserStackEntry struct {
	start int
	node  vlQueryNode
}

type vlQueryParserStack []*vlQueryParserStackEntry

func (s *vlQueryParserStack) pop() *vlQueryParserStackEntry {
	if len(*s) == 0 {
		return nil
	}
	e := (*s)[len(*s)-1]
	*s = (*s)[:len(*s)-1]
	return e
}

func (s *vlQueryParserStack) last() *vlQueryParserStackEntry {
	if len(*s) == 0 {
		return nil
	}
	e := (*s)[len(*s)-1]
	return e
}
func (s *vlQueryParserStack) push(entry *vlQueryParserStackEntry) {
	*s = append(*s, entry)
}
func (s *vlQueryParserStack) depth() int { return len(*s) }

func parseValueLists(query string) (*vlQuery, error) {
	var nodes vlQuery
	var stack = vlQueryParserStack(make([]*vlQueryParserStackEntry, 0, 8))

	for cur, c := range query {
		// if *qvlDebug {
		// 	log.Printf("%s @ %d: stack = %+v\n", query[:cur+1], cur, stack)
		// 	pretty.Println(stack)
		// }

		switch c {
		case '{':
			curEntry := stack.last()
			if curEntry == nil {
				stack.push(&vlQueryParserStackEntry{start: cur, node: &vlQueryList{}})
				continue
			}

			switch node := curEntry.node.(type) {
			case *vlQuery:
				subl := &vlQueryList{}
				*node = append(*node, subl)
				stack.push(&vlQueryParserStackEntry{start: cur, node: subl})
			case *vlQueryList:
				subl := &vlQueryList{}

				if len(*node) == 0 || (cur > 1 && query[cur-1] == ',') {
					*node = append(*node, subl)
				} else {
					if len(*node) > 0 {
						switch sibling := (*node)[len(*node)-1].(type) {
						case *vlQuery:
							*sibling = append(*sibling, subl)
							stack.push(&vlQueryParserStackEntry{start: cur, node: sibling})
						case *vlQueryList:
							subq := &vlQuery{sibling, subl}
							(*node)[len(*node)-1] = subq
							stack.push(&vlQueryParserStackEntry{start: cur, node: subq})
						}
					} else {
						subq := &vlQuery{node, subl}
						curEntry.node = subq
					}
				}

				stack.push(&vlQueryParserStackEntry{start: cur, node: subl})
			case vlQueryLiteral:
				literal := vlQueryLiteral(query[curEntry.start:cur])

				if stack.depth() == 1 {
					nodes = append(nodes, literal)
					stack.pop()

					stack.push(&vlQueryParserStackEntry{start: cur, node: &vlQueryList{}})
				} else if stack.depth() > 1 {
					subl := &vlQueryList{}

					switch parent := stack[stack.depth()-2].node.(type) {
					case *vlQuery:
						*parent = append(*parent, literal, subl)
						stack.pop()
					case *vlQueryList:
						subq := &vlQuery{literal, subl}
						curEntry.node = subq
						*parent = append(*parent, subq)
					}

					stack.push(&vlQueryParserStackEntry{start: cur, node: subl})
				}
			case nil:
				// TODO: impossible?
				stack.push(&vlQueryParserStackEntry{start: cur, node: &vlQueryList{}})
			}
		case ',':
			curEntry := stack.last()

			// TODO: should check if its outside of value list?
			if curEntry == nil {
				// TODO: should return error instead?
				stack.push(&vlQueryParserStackEntry{start: cur, node: vlQueryLiteral("")})

				continue
			}

			switch node := curEntry.node.(type) {
			case *vlQuery:
				stack.pop()
			case *vlQueryList:
				// empty string literal, do nothing
				// stack = append(stack, &entry{start: cur, node: &vlQueryLiteral("")})

				// for supporting empty value item like '{,}'
				if len(*node) == 0 || (cur > 1 && query[cur-1] == ',') {
					*node = append(*node, vlQueryLiteral(""))
				}
			case vlQueryLiteral:
				if stack.depth() == 1 {
					// most likely due to cases like ",,,"
					continue
				}

				literal := vlQueryLiteral(query[curEntry.start:cur])
				stack.pop()
				parentEntry := stack.last()

				switch parent := parentEntry.node.(type) {
				case *vlQueryList:
					*parent = append(*parent, literal)
				case *vlQuery:
					*parent = append(*parent, literal)
					stack.pop()

					// TODO: should check if parent of parent not a value list?
				default:
					// return
				}
			}
		case '}':
			// TODO: supporting empty value item like '{,}'

			curEntry := stack.last()
			if curEntry == nil {
				// TODO: return error
				return nil, errors.New("query has an unpaired closing curly bracket")
			}

			switch node := curEntry.node.(type) {
			case *vlQuery:
				stack.pop()

				parentEntry := stack.pop()
				if parentEntry == nil {
					return nil, errors.New("found vlQuery outside of value list")
				}

				if stack.depth() == 0 {
					nodes = append(nodes, parentEntry.node)
				}
			case *vlQueryList:
				// empty string literal, do nothing
				if cur > 0 && query[cur-1] == ',' {
					*node = append(*node, vlQueryLiteral(""))
				}

				stack.pop()
				if stack.depth() == 0 {
					nodes = append(nodes, node)
				}
			case vlQueryLiteral:
				literal := vlQueryLiteral(query[curEntry.start:cur])
				stack.pop()

				parentEntry := stack.last()
				if parentEntry == nil {
					return nil, errors.New("query has an unpaired closing curly bracket")
				}

				switch parent := parentEntry.node.(type) {
				case *vlQuery:
					stack.pop()
					*parent = append(*parent, literal)

					grandParentEntry := stack.pop()
					if grandParentEntry == nil {
						return nil, errors.New("found vlQuery outside of value list")
					}

					if stack.depth() == 0 {
						nodes = append(nodes, grandParentEntry.node)
					}
				case *vlQueryList:
					*parent = append(*parent, literal)

					stack.pop()
					if stack.depth() == 0 {
						nodes = append(nodes, parent)
					}

				// TODO: should check if parent of parent not a value list?
				default:
					// TODO: error?
				}
			}

			// TODO: handling spaces?
		default:
			if stack.depth() == 0 {
				stack.push(&vlQueryParserStackEntry{start: cur, node: vlQueryLiteral("")})
				continue
			}

			lastEntry := stack.last()

			switch node := lastEntry.node.(type) {
			case *vlQuery:
				stack.push(&vlQueryParserStackEntry{start: cur, node: vlQueryLiteral("")})
			case *vlQueryList:
				if len(*node) == 0 {
					// stack.push(&vlQueryParserStackEntry{start: cur, node: vlQueryLiteral("")})
				} else if sublist, ok := (*node)[len(*node)-1].(*vlQueryList); ok {
					var delimited bool
					if cur > 0 && query[cur-1] == ',' {
						delimited = true
					}

					if !delimited {
						subq := &vlQuery{sublist}
						(*node)[len(*node)-1] = subq
						stack.push(&vlQueryParserStackEntry{start: cur, node: subq})
					}
				}

				stack.push(&vlQueryParserStackEntry{start: cur, node: vlQueryLiteral("")})
			}
		}
	}

	// if *qvlDebug {
	// 	log.Printf("end: stack = %+v\n", stack)
	// 	pretty.Println(stack)
	// }

	if stack.last() != nil {
		curEntry := stack.pop()
		switch curEntry.node.(type) {
		case vlQueryLiteral:
			nodes = append(nodes, vlQueryLiteral(query[curEntry.start:]))
		case *vlQueryList:
			return nil, errors.New("query has an unpaired opening curly bracket")
		default:
			nodes = append(nodes, curEntry.node)
		}
	}

	if stack.depth() > 0 {
		return nil, errors.New("query has an unpaired opening curly bracket")
	}

	return &nodes, nil
}
