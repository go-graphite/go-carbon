package carbonserver

import (
	"errors"
	"regexp"
	"runtime/debug"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func FuzzValueListParsingRoundTrip(f *testing.F) {
	f.Add(`{`)
	f.Add(`}`)
	f.Add(`,`)
	f.Add(`.`)
	f.Add(`{xyz,abc}`)
	f.Add(`{xyz.abc}.abc`)
	f.Add(`test.{abc,xyz}.abc`)
	f.Fuzz(func(t *testing.T, query string) {
		defer func() {
			if x := recover(); x != nil {
				t.Errorf("panics catched (%s): %s\n%s", query, x, debug.Stack())
			}
		}()

		vlq, err := parseValueLists(query)
		if err != nil {
			return
		}
		if vlqs := vlq.String(); vlqs != query {
			t.Errorf("round trip parsing and encoding failed for query: %s (got: %s)", query, vlqs)
		}
	})
}

func TestValueListQueryStringer(t *testing.T) {
	query := &vlQuery{
		vlQueryLiteral("test."),
		&vlQueryList{
			vlQueryLiteral("test"),
			vlQueryLiteral("abc"),
			vlQueryLiteral("abc.xyz"),
			&vlQueryList{
				vlQueryLiteral("test"),
				vlQueryLiteral("abc"),
				vlQueryLiteral("abc.xyz"),
			},
			&vlQuery{ // for encoding
				vlQueryLiteral("sub."),
				&vlQueryList{
					vlQueryLiteral("test"),
					vlQueryLiteral("abc"),
				},
				// &vlQueryList{
				// 		vlQueryLiteral("test"),
				// 		vlQueryLiteral("abc"),
				// },
			},
			&vlQuery{
				vlQueryLiteral("sub."),
				&vlQueryList{
					vlQueryLiteral("test"),
					&vlQueryList{
						&vlQueryList{
							vlQueryLiteral("test"),
							vlQueryLiteral("abc"),
						},
					},
				},
				vlQueryLiteral(".suffix"),
			},
			&vlQueryList{
				&vlQueryList{
					&vlQueryList{
						&vlQueryList{
							vlQueryLiteral("test"),
							vlQueryLiteral("abc"),
						},
					},
				},
			},
			&vlQueryList{
				&vlQueryList{
					&vlQueryList{
						&vlQuery{
							&vlQueryList{
								vlQueryLiteral("test"),
								vlQueryLiteral("abc"),
							},
							vlQueryLiteral(".suffix"),
						},
					},
				},
			},
		},
		vlQueryLiteral(".test."),
		&vlQueryList{
			&vlQueryList{
				&vlQueryList{
					&vlQuery{
						&vlQueryList{
							vlQueryLiteral("test"),
							vlQueryLiteral("abc"),
						},
						vlQueryLiteral(".suffix"),
					},
				},
			},
		},
	}
	if got, want := query.String(), `test.{test,abc,abc.xyz,{test,abc,abc.xyz},sub.{test,abc},sub.{test,{{test,abc}}}.suffix,{{{{test,abc}}}},{{{{test,abc}.suffix}}}}.test.{{{{test,abc}.suffix}}}`; got != want {
		t.Errorf("recording failed:\nquery.String() = %s\nwant             %s", got, want)
	}
}

// var qvlDebug = flag.Bool("qvlDebug", false, "print parseValueLists debug info")

func TestValueListQueryParser(t *testing.T) {
	// test.{abc,xyz}.abc
	// {{{test,abc},xyz.abc},xyz.abc}.abc
	// {,test,abc}.test
	// prefix.{,test,abc}test
	// {test.abc.xyz}
	// {test}

	for _, tcase := range []struct {
		query  string
		output *vlQuery
		err    error
	}{
		// overly_suspicious_testcase -- start
		//
		// probably controversal to support queries like this,
		// but in trie-indexing, it shouldn't reach value list parser.
		{
			query:  `,`,
			output: &vlQuery{vlQueryLiteral(",")},
		},
		{
			query:  `,,,,`,
			output: &vlQuery{vlQueryLiteral(",,,,")},
		},
		{
			query: `{`,
			err:   errors.New("query has an unpaired opening curly bracket"),
		},
		{
			query: `{{{`,
			err:   errors.New("query has an unpaired opening curly bracket"),
		},
		{
			query: `}`,
			err:   errors.New("query has an unpaired closing curly bracket"),
		},
		{
			query: `}}}`,
			err:   errors.New("query has an unpaired closing curly bracket"),
		},
		{
			query:  `.`,
			output: &vlQuery{vlQueryLiteral(".")},
		},
		{
			query: `0}`,
			err:   errors.New("query has an unpaired closing curly bracket"),
			// output: &vlQuery{vlQueryLiteral(".")},
		},
		{
			query: `{{}0{`,
			err:   errors.New("query has an unpaired opening curly bracket"),
			// output: &vlQuery{vlQueryLiteral(".")},
		},
		{
			query: `{0{}0}`,
			// err:   errors.New("query has an unpaired closing curly bracket"),
			output: &vlQuery{
				&vlQueryList{
					&vlQuery{
						vlQueryLiteral("0"),
						&vlQueryList{},
						vlQueryLiteral("0"),
					},
				},
			},
		},
		{
			query: `{0{}0{}}`,
			// err:   errors.New("query has an unpaired closing curly bracket"),
			output: &vlQuery{
				&vlQueryList{
					&vlQuery{
						vlQueryLiteral("0"),
						&vlQueryList{},
						vlQueryLiteral("0"),
						&vlQueryList{},
					},
				},
			},
		},
		{
			query: `{{}{}}`,
			// err:   errors.New("query has an unpaired closing curly bracket"),
			output: &vlQuery{
				&vlQueryList{
					&vlQuery{
						&vlQueryList{},
						&vlQueryList{},
					},
				},
			},
		},
		{
			query: `{{},{}}`,
			// err:   errors.New("query has an unpaired closing curly bracket"),
			output: &vlQuery{
				&vlQueryList{
					&vlQueryList{},
					&vlQueryList{},
				},
			},
		},
		{
			query: `{}{}`,
			// err:   errors.New("query has an unpaired closing curly bracket"),
			output: &vlQuery{
				&vlQueryList{},
				&vlQueryList{},
			},
		},
		{
			query: `{}.{}`,
			// err:   errors.New("query has an unpaired closing curly bracket"),
			output: &vlQuery{
				&vlQueryList{},
				vlQueryLiteral("."),
				&vlQueryList{},
			},
		},
		{
			query: `{{}{}0}`,
			output: &vlQuery{
				&vlQueryList{
					&vlQuery{
						&vlQueryList{},
						&vlQueryList{},
						vlQueryLiteral("0"),
					},
				},
			},
		},
		{
			query: `{,}`,
			output: &vlQuery{
				&vlQueryList{
					vlQueryLiteral(""),
					vlQueryLiteral(""),
				},
			},
		},
		{
			query: `{0{,}`,
			err:   errors.New("query has an unpaired opening curly bracket"),
		},
		{
			query: `{0{,z}`,
			err:   errors.New("query has an unpaired opening curly bracket"),
		},
		{
			query: `{0{,}}`,
			// err:   errors.New("query has an unpaired closing curly bracket"),
			output: &vlQuery{
				&vlQueryList{
					&vlQuery{
						vlQueryLiteral("0"),
						&vlQueryList{
							vlQueryLiteral(""),
							vlQueryLiteral(""),
						},
					},
				},
			},
		},
		{
			query: `{0{,z}}`,
			// err:   errors.New("query has an unpaired closing curly bracket"),
			output: &vlQuery{
				&vlQueryList{
					&vlQuery{
						vlQueryLiteral("0"),
						&vlQueryList{
							vlQueryLiteral(""),
							vlQueryLiteral("z"),
						},
					},
				},
			},
		},
		{
			query: `{{}{{}}`,
			err:   errors.New("query has an unpaired opening curly bracket"),
		},
		{
			query: `{{}{{}}}`,
			output: &vlQuery{
				&vlQueryList{
					&vlQuery{
						&vlQueryList{},
						&vlQueryList{
							&vlQueryList{},
						},
					},
				},
			},
		},
		{
			query: `{{}{{}}.xyz}.xyz`,
			output: &vlQuery{
				&vlQueryList{
					&vlQuery{
						&vlQueryList{},
						&vlQueryList{
							&vlQueryList{},
						},
						vlQueryLiteral(".xyz"),
					},
				},
				vlQueryLiteral(".xyz"),
			},
		},
		{
			query: `{{}{{},}{}`,
			err:   errors.New("query has an unpaired opening curly bracket"),
		},
		{
			query: `{{}{{},}{}}`,
			output: &vlQuery{
				&vlQueryList{
					&vlQuery{
						&vlQueryList{},
						&vlQueryList{
							&vlQueryList{},
							vlQueryLiteral(""),
						},
						&vlQueryList{},
					},
				},
			},
		},
		{
			query: `{{}{{}{},}{}}`,
			output: &vlQuery{
				&vlQueryList{
					&vlQuery{
						&vlQueryList{},
						&vlQueryList{
							&vlQuery{
								&vlQueryList{},
								&vlQueryList{},
							},
							vlQueryLiteral(""),
						},
						&vlQueryList{},
					},
				},
			},
		},
		{
			query: `{{}{{}test{},}{{}{},{}{{}{},{{{}{}{}{},{}{}}}}},test}.test`,
			output: &vlQuery{
				&vlQueryList{
					&vlQuery{
						&vlQueryList{},
						&vlQueryList{
							&vlQuery{
								&vlQueryList{},
								vlQueryLiteral("test"),
								&vlQueryList{},
							},
							vlQueryLiteral(""),
						},
						&vlQueryList{
							&vlQuery{
								&vlQueryList{},
								&vlQueryList{},
							},
							&vlQuery{
								&vlQueryList{},
								&vlQueryList{
									&vlQuery{
										&vlQueryList{},
										&vlQueryList{},
									},
									&vlQueryList{
										&vlQueryList{
											&vlQuery{
												&vlQueryList{},
												&vlQueryList{},
												&vlQueryList{},
												&vlQueryList{},
											},
											&vlQuery{
												&vlQueryList{},
												&vlQueryList{},
											},
										},
									},
								},
							},
						},
					},
					vlQueryLiteral("test"),
				},
				vlQueryLiteral(".test"),
			},
		},
		{
			query: `{{xyz,abc.xdb}{{xyz,abc,xyz.abc}test{abc.abc,xyz},}{{xyz,abc.xyz}{abc,xyz},{abc,xyz.abc}{{abc.xyz,abc}{abc.xyz},{{{abc.xyz}{abc.xyz,abc,xyz}{abc,xyz,abc.xyz}{abc,xyz,abc},{abc,xyz}{abc,xyz,abc}}}}},test}.test`,
			output: &vlQuery{
				&vlQueryList{
					&vlQuery{
						&vlQueryList{
							vlQueryLiteral("xyz"),
							vlQueryLiteral("abc.xdb"),
						},
						&vlQueryList{
							&vlQuery{
								&vlQueryList{
									vlQueryLiteral("xyz"),
									vlQueryLiteral("abc"),
									vlQueryLiteral("xyz.abc"),
								},
								vlQueryLiteral("test"),
								&vlQueryList{
									vlQueryLiteral("abc.abc"),
									vlQueryLiteral("xyz"),
								},
							},
							vlQueryLiteral(""),
						},
						&vlQueryList{
							&vlQuery{
								&vlQueryList{
									vlQueryLiteral("xyz"),
									vlQueryLiteral("abc.xyz"),
								},
								&vlQueryList{
									vlQueryLiteral("abc"),
									vlQueryLiteral("xyz"),
								},
							},
							&vlQuery{
								&vlQueryList{
									vlQueryLiteral("abc"),
									vlQueryLiteral("xyz.abc"),
								},
								&vlQueryList{
									&vlQuery{
										&vlQueryList{
											vlQueryLiteral("abc.xyz"),
											vlQueryLiteral("abc"),
										},
										&vlQueryList{
											vlQueryLiteral("abc.xyz"),
										},
									},
									&vlQueryList{
										&vlQueryList{
											&vlQuery{
												&vlQueryList{
													vlQueryLiteral("abc.xyz"),
												},
												&vlQueryList{
													vlQueryLiteral("abc.xyz"),
													vlQueryLiteral("abc"),
													vlQueryLiteral("xyz"),
												},
												&vlQueryList{
													vlQueryLiteral("abc"),
													vlQueryLiteral("xyz"),
													vlQueryLiteral("abc.xyz"),
												},
												&vlQueryList{
													vlQueryLiteral("abc"),
													vlQueryLiteral("xyz"),
													vlQueryLiteral("abc"),
												},
											},
											&vlQuery{
												&vlQueryList{
													vlQueryLiteral("abc"),
													vlQueryLiteral("xyz"),
												},
												&vlQueryList{
													vlQueryLiteral("abc"),
													vlQueryLiteral("xyz"),
													vlQueryLiteral("abc"),
												},
											},
										},
									},
								},
							},
						},
					},
					vlQueryLiteral("test"),
				},
				vlQueryLiteral(".test"),
			},
		},
		// overly_suspicious_testcase -- end

		{
			query: `test.{abc,xyz}.abc`,
			output: &vlQuery{
				vlQueryLiteral("test."),
				&vlQueryList{
					vlQueryLiteral("abc"),
					vlQueryLiteral("xyz"),
				},
				vlQueryLiteral(".abc"),
			},
		},
		{
			query: `test.{abc,xyz,石墨}.abc`,
			output: &vlQuery{
				vlQueryLiteral("test."),
				&vlQueryList{
					vlQueryLiteral("abc"),
					vlQueryLiteral("xyz"),
					vlQueryLiteral("石墨"),
				},
				vlQueryLiteral(".abc"),
			},
		},
		{
			query: `test.{abc,{xyz}}.abc`,
			output: &vlQuery{
				vlQueryLiteral("test."),
				&vlQueryList{
					vlQueryLiteral("abc"),
					&vlQueryList{
						vlQueryLiteral("xyz"),
					},
				},
				vlQueryLiteral(".abc"),
			},
		},
		{
			query: `test.{abc,{xyz,abc},abc}.abc`,
			output: &vlQuery{
				vlQueryLiteral("test."),
				&vlQueryList{
					vlQueryLiteral("abc"),
					&vlQueryList{
						vlQueryLiteral("xyz"),
						vlQueryLiteral("abc"),
					},
					vlQueryLiteral("abc"),
				},
				vlQueryLiteral(".abc"),
			},
		},
		{
			query: `prefix.{,test,abc}test`,
			output: &vlQuery{
				vlQueryLiteral("prefix."),
				&vlQueryList{
					vlQueryLiteral(""),
					vlQueryLiteral("test"),
					vlQueryLiteral("abc"),
				},
				vlQueryLiteral("test"),
			},
		},
		{
			query: `prefix.{,test,{abc}.abc}test`,
			output: &vlQuery{
				vlQueryLiteral("prefix."),
				&vlQueryList{
					vlQueryLiteral(""),
					vlQueryLiteral("test"),
					&vlQuery{
						&vlQueryList{
							vlQueryLiteral("abc"),
						},
						vlQueryLiteral(".abc"),
					},
				},
				vlQueryLiteral("test"),
			},
		},
		{
			query: `prefix.{,test,{abc}.abc,xyz}test`,
			output: &vlQuery{
				vlQueryLiteral("prefix."),
				&vlQueryList{
					vlQueryLiteral(""),
					vlQueryLiteral("test"),
					&vlQuery{
						&vlQueryList{
							vlQueryLiteral("abc"),
						},
						vlQueryLiteral(".abc"),
					},
					vlQueryLiteral("xyz"),
				},
				vlQueryLiteral("test"),
			},
		},
		{
			query: `prefix.{,test,{{abc}.abc,xyz}test`,
			err:   errors.New("query has an unpaired opening curly bracket"),
		},
		{
			query: `prefix.{,test,{abc}.abc}},xyz}test`,
			err:   errors.New("query has an unpaired closing curly bracket"),
		},
		{
			query: `{{{test,abc},xyz.abc},xyz.abc}.abc`,
			output: &vlQuery{
				&vlQueryList{
					&vlQueryList{
						&vlQueryList{
							vlQueryLiteral("test"),
							vlQueryLiteral("abc"),
						},
						vlQueryLiteral("xyz.abc"),
					},
					vlQueryLiteral("xyz.abc"),
				},
				vlQueryLiteral(".abc"),
			},
		},
		{
			query: `{{{test,abc},xyz.abc},xyz.abc}.abc.{{{test,abc},xyz.abc},xyz.abc}.abc`,
			output: &vlQuery{
				&vlQueryList{
					&vlQueryList{
						&vlQueryList{
							vlQueryLiteral("test"),
							vlQueryLiteral("abc"),
						},
						vlQueryLiteral("xyz.abc"),
					},
					vlQueryLiteral("xyz.abc"),
				},
				vlQueryLiteral(".abc."),
				&vlQueryList{
					&vlQueryList{
						&vlQueryList{
							vlQueryLiteral("test"),
							vlQueryLiteral("abc"),
						},
						vlQueryLiteral("xyz.abc"),
					},
					vlQueryLiteral("xyz.abc"),
				},
				vlQueryLiteral(".abc"),
			},
		},
		{
			query: `{test,,test}.abc`,
			output: &vlQuery{
				&vlQueryList{
					vlQueryLiteral("test"),
					vlQueryLiteral(""),
					vlQueryLiteral("test"),
				},
				vlQueryLiteral(".abc"),
			},
		},
		{
			query: `}{test,,test}.abc`,
			err:   errors.New("query has an unpaired closing curly bracket"),
		},
		{
			query: `{{test,,test}.abc`,
			err:   errors.New("query has an unpaired opening curly bracket"),
		},
		{
			query: `{test,abc,}.abc`,
			output: &vlQuery{
				&vlQueryList{
					vlQueryLiteral("test"),
					vlQueryLiteral("abc"),
					vlQueryLiteral(""),
				},
				vlQueryLiteral(".abc"),
			},
		},
		{
			query: `{,,}.abc`,
			output: &vlQuery{
				&vlQueryList{
					vlQueryLiteral(""),
					vlQueryLiteral(""),
					vlQueryLiteral(""),
				},
				vlQueryLiteral(".abc"),
			},
		},
		{
			query: `{{{test,abc},xyz.abc},xyz.abc}.abc`,
			output: &vlQuery{
				&vlQueryList{
					&vlQueryList{
						&vlQueryList{
							vlQueryLiteral("test"),
							vlQueryLiteral("abc"),
						},
						vlQueryLiteral("xyz.abc"),
					},
					vlQueryLiteral("xyz.abc"),
				},
				vlQueryLiteral(".abc"),
			},
		},
		{
			query: `{{{test,abc},xyz.abc},xyz.abc{xyz,obj}}.abc`,
			output: &vlQuery{
				&vlQueryList{
					&vlQueryList{
						&vlQueryList{
							vlQueryLiteral("test"),
							vlQueryLiteral("abc"),
						},
						vlQueryLiteral("xyz.abc"),
					},
					&vlQuery{
						vlQueryLiteral("xyz.abc"),
						&vlQueryList{
							vlQueryLiteral("xyz"),
							vlQueryLiteral("obj"),
						},
					},
				},
				vlQueryLiteral(".abc"),
			},
		},
		{
			query: `{{{test,abc},xyz.abc},xyz.abc{xyz,obj},xxx}.abc`,
			output: &vlQuery{
				&vlQueryList{
					&vlQueryList{
						&vlQueryList{
							vlQueryLiteral("test"),
							vlQueryLiteral("abc"),
						},
						vlQueryLiteral("xyz.abc"),
					},
					&vlQuery{
						vlQueryLiteral("xyz.abc"),
						&vlQueryList{
							vlQueryLiteral("xyz"),
							vlQueryLiteral("obj"),
						},
					},
					vlQueryLiteral("xxx"),
				},
				vlQueryLiteral(".abc"),
			},
		},
		{
			query: `a.b.c.{{eu-central1-a,eu-central2a,eu-west1-a,as-southeast1-a,as-east1-a,eu-west2-a,eu-west6a},by_az.{eu-central1-a,eu-central2a,eu-west1-a,as-southeast1-a,as-east1-a,eu-west2-a,eu-west6a}}.d.e.f.*.g.sum`,
			output: &vlQuery{
				vlQueryLiteral("a.b.c."),
				&vlQueryList{
					&vlQueryList{
						vlQueryLiteral("eu-central1-a"),
						vlQueryLiteral("eu-central2a"),
						vlQueryLiteral("eu-west1-a"),
						vlQueryLiteral("as-southeast1-a"),
						vlQueryLiteral("as-east1-a"),
						vlQueryLiteral("eu-west2-a"),
						vlQueryLiteral("eu-west6a"),
					},
					&vlQuery{
						vlQueryLiteral("by_az."),
						&vlQueryList{
							vlQueryLiteral("eu-central1-a"),
							vlQueryLiteral("eu-central2a"),
							vlQueryLiteral("eu-west1-a"),
							vlQueryLiteral("as-southeast1-a"),
							vlQueryLiteral("as-east1-a"),
							vlQueryLiteral("eu-west2-a"),
							vlQueryLiteral("eu-west6a"),
						},
					},
				},
				vlQueryLiteral(".d.e.f.*.g.sum"),
			},
		},
		{
			query: `{{{test,abc},xyz.abc},,xyz.abc{xyz,obj},,xxx,}.abc`,
			output: &vlQuery{
				&vlQueryList{
					&vlQueryList{
						&vlQueryList{
							vlQueryLiteral("test"),
							vlQueryLiteral("abc"),
						},
						vlQueryLiteral("xyz.abc"),
					},
					vlQueryLiteral(""),
					&vlQuery{
						vlQueryLiteral("xyz.abc"),
						&vlQueryList{
							vlQueryLiteral("xyz"),
							vlQueryLiteral("obj"),
						},
					},
					vlQueryLiteral(""),
					vlQueryLiteral("xxx"),
					vlQueryLiteral(""),
				},
				vlQueryLiteral(".abc"),
			},
		},
		{
			query: "test.{foo1,foo2,sub.{foo1,foo2.xyz{abc,xyz.abc,{xyz,abc.xyz}}}}.test.{foo1,foo2,sub.{foo1,foo2.xyz{abc,xyz.abc,{xyz,abc.xyz}}}}",
			output: &vlQuery{
				vlQueryLiteral("test."),
				&vlQueryList{
					vlQueryLiteral("foo1"),
					vlQueryLiteral("foo2"),
					&vlQuery{
						vlQueryLiteral("sub."),
						&vlQueryList{
							vlQueryLiteral("foo1"),
							&vlQuery{
								vlQueryLiteral("foo2.xyz"),
								&vlQueryList{
									vlQueryLiteral("abc"),
									vlQueryLiteral("xyz.abc"),
									&vlQueryList{
										vlQueryLiteral("xyz"),
										vlQueryLiteral("abc.xyz"),
									},
								},
							},
						},
					},
				},
				vlQueryLiteral(".test."),
				&vlQueryList{
					vlQueryLiteral("foo1"),
					vlQueryLiteral("foo2"),
					&vlQuery{
						vlQueryLiteral("sub."),
						&vlQueryList{
							vlQueryLiteral("foo1"),
							&vlQuery{
								vlQueryLiteral("foo2.xyz"),
								&vlQueryList{
									vlQueryLiteral("abc"),
									vlQueryLiteral("xyz.abc"),
									&vlQueryList{
										vlQueryLiteral("xyz"),
										vlQueryLiteral("abc.xyz"),
									},
								},
							},
						},
					},
				},
			},
		},
	} {
		t.Run(tcase.query, func(t *testing.T) {
			t.Logf("case: TestValueListQueryParser/'^%s$'", regexp.QuoteMeta(tcase.query))

			vlq, err := parseValueLists(tcase.query)
			if (err != nil || tcase.err != nil) && ((err == nil) || (tcase.err == nil) || (err.Error() != tcase.err.Error())) {
				t.Errorf("err = %s; want %s", err, tcase.err)
			}

			// log.Printf("query = %+v\n", tcase.query)
			// log.Printf("%#v\n", vlq)
			// pretty.Printf("%# v\n", vlq)

			if diff := cmp.Diff(vlq, tcase.output); diff != "" {
				t.Errorf("diff: %s", diff)
			}

			if vlq != nil {
				if got, want := vlq.String(), tcase.query; got != want {
					t.Errorf("vlq.String() = %s; want %s", got, want)
				}
			}
		})
	}

	// query :=
	// vlq, err := parseValueLists(query)
	// log.Printf("query = %+v\n", query)
	// log.Printf("err = %+v\n", err)
	// pretty.Println(vlq)

	// query := `{{{test,abc},xyz.abc},xyz.abc}.abc`
	// vlq, err := parseValueLists(query)
	// log.Printf("query = %+v\n", query)
	// log.Printf("err = %+v\n", err)
	// pretty.Println(vlq)
}

func TestValueListQueryRewrite(t *testing.T) {
	for _, tcase := range []struct {
		name   string
		query  *vlQuery
		output []string
	}{
		{
			name: "test.{test,abc,abc.xyz}{test,abc,abc.xyz}.test",
			query: &vlQuery{
				vlQueryLiteral("test."),
				&vlQueryList{
					vlQueryLiteral("test"),
					vlQueryLiteral("abc"),
					vlQueryLiteral("abc.xyz"),
				},
				&vlQueryList{
					vlQueryLiteral("test"),
					vlQueryLiteral("abc"),
					vlQueryLiteral("abc.xyz"),
				},
				vlQueryLiteral(".test"),
			},
			output: []string{
				"test.{test,abc}{test,abc}.test",
				"test.{test,abc}abc.xyz.test",
				"test.abc.xyz{test,abc}.test",
				"test.abc.xyzabc.xyz.test",
			},
		},
		{
			name: "test.{test,abc,abc.xyz,{test1,abc1,abc1.xyz1}}.test",
			query: &vlQuery{
				vlQueryLiteral("test."),
				&vlQueryList{
					vlQueryLiteral("test"),
					vlQueryLiteral("abc"),
					vlQueryLiteral("abc.xyz"),
					&vlQueryList{
						vlQueryLiteral("test1"),
						vlQueryLiteral("abc1"),
						vlQueryLiteral("abc1.xyz1"),
					},
				},
				vlQueryLiteral(".test"),
			},
			output: []string{
				"test.{test,abc,test1,abc1}.test",
				"test.abc.xyz.test",
				"test.abc1.xyz1.test",
			},
		},
		{
			name: "test.{test,abc,abc.xyz,{sub.{test1,abc1,abc1.xyz1}}}.test",
			query: &vlQuery{
				vlQueryLiteral("test."),
				&vlQueryList{
					vlQueryLiteral("test"),
					vlQueryLiteral("abc"),
					vlQueryLiteral("abc.xyz"),
					&vlQueryList{
						&vlQuery{
							vlQueryLiteral("sub."),
							&vlQueryList{
								vlQueryLiteral("test1"),
								vlQueryLiteral("abc1"),
								vlQueryLiteral("abc1.xyz1"), // incorrect
							},
						},
					},
				},
				vlQueryLiteral(".test"),
			},
			output: []string{
				"test.{test,abc}.test",
				"test.sub.{test1,abc1}.test",
				"test.sub.abc1.xyz1.test",
				"test.abc.xyz.test",
			},
		},
		{
			name: "test.{foo1,foo2,sub.{foo1,foo2}}.test",
			query: &vlQuery{
				vlQueryLiteral("test."),
				&vlQueryList{
					vlQueryLiteral("foo1"),
					vlQueryLiteral("foo2"),
					&vlQuery{
						vlQueryLiteral("sub."),
						&vlQueryList{
							vlQueryLiteral("foo1"),
							vlQueryLiteral("foo2"),
						},
					},
				},
				vlQueryLiteral(".test"),
			},
			output: []string{
				"test.{foo1,foo2}.test",
				"test.sub.{foo1,foo2}.test",
			},
		},
		{
			name: "test.{foo1,foo2,sub.{foo1,foo2.xyz{abc,xyz.abc,{xyz,abc.xyz}}}}.test",
			query: &vlQuery{
				vlQueryLiteral("test."),
				&vlQueryList{
					vlQueryLiteral("foo1"),
					vlQueryLiteral("foo2"),
					&vlQuery{
						vlQueryLiteral("sub."),
						&vlQueryList{
							vlQueryLiteral("foo1"),
							&vlQuery{
								vlQueryLiteral("foo2.xyz"),
								&vlQueryList{
									vlQueryLiteral("abc"),
									vlQueryLiteral("xyz.abc"),
									&vlQueryList{
										vlQueryLiteral("xyz"),
										vlQueryLiteral("abc.xyz"),
									},
								},
							},
						},
					},
				},
				vlQueryLiteral(".test"),
			},
			output: []string{
				"test.{foo1,foo2}.test",
				"test.sub.foo1.test",
				"test.sub.foo2.xyz{abc,xyz}.test",
				"test.sub.foo2.xyzxyz.abc.test",
				"test.sub.foo2.xyzabc.xyz.test",
			},
		},
		{
			name: `a.b.c.{{eu-central1-a,eu-central2a,eu-west1-a,as-southeast1-a,as-east1-a,eu-west2-a,eu-west6a},by_az.{eu-central1-a,eu-central2a,eu-west1-a,as-southeast1-a,as-east1-a,eu-west2-a,eu-west6a}}.d.e.f.*.g.sum`,
			query: &vlQuery{
				vlQueryLiteral("a.b.c."),
				&vlQueryList{
					&vlQueryList{
						vlQueryLiteral("eu-central1-a"),
						vlQueryLiteral("eu-central2a"),
						vlQueryLiteral("eu-west1-a"),
						vlQueryLiteral("as-southeast1-a"),
						vlQueryLiteral("as-east1-a"),
						vlQueryLiteral("eu-west2-a"),
						vlQueryLiteral("eu-west6a"),
					},
					&vlQuery{
						vlQueryLiteral("by_az."),
						&vlQueryList{
							vlQueryLiteral("eu-central1-a"),
							vlQueryLiteral("eu-central2a"),
							vlQueryLiteral("eu-west1-a"),
							vlQueryLiteral("as-southeast1-a"),
							vlQueryLiteral("as-east1-a"),
							vlQueryLiteral("eu-west2-a"),
							vlQueryLiteral("eu-west6a"),
						},
					},
				},
				vlQueryLiteral(".d.e.f.*.g.sum"),
			},
			output: []string{
				"a.b.c.{eu-central1-a,eu-central2a,eu-west1-a,as-southeast1-a,as-east1-a,eu-west2-a,eu-west6a}.d.e.f.*.g.sum",
				"a.b.c.by_az.{eu-central1-a,eu-central2a,eu-west1-a,as-southeast1-a,as-east1-a,eu-west2-a,eu-west6a}.d.e.f.*.g.sum",
			},
		},
		{
			name: "test.{foo1,foo2,sub.{foo1,foo2.xyz{abc,xyz.abc,{xyz,abc.xyz}}}}.test.{foo1,foo2,sub.{foo1,foo2.xyz{abc,xyz.abc,{xyz,abc.xyz}}}}",
			query: &vlQuery{
				vlQueryLiteral("test."),
				&vlQueryList{
					vlQueryLiteral("foo1"),
					vlQueryLiteral("foo2"),
					&vlQuery{
						vlQueryLiteral("sub."),
						&vlQueryList{
							vlQueryLiteral("foo1"),
							&vlQuery{
								vlQueryLiteral("foo2.xyz"),
								&vlQueryList{
									vlQueryLiteral("abc"),
									vlQueryLiteral("xyz.abc"),
									&vlQueryList{
										vlQueryLiteral("xyz"),
										vlQueryLiteral("abc.xyz"),
									},
								},
							},
						},
					},
				},
				vlQueryLiteral(".test."),
				&vlQueryList{
					vlQueryLiteral("foo1"),
					vlQueryLiteral("foo2"),
					&vlQuery{
						vlQueryLiteral("sub."),
						&vlQueryList{
							vlQueryLiteral("foo1"),
							&vlQuery{
								vlQueryLiteral("foo2.xyz"),
								&vlQueryList{
									vlQueryLiteral("abc"),
									vlQueryLiteral("xyz.abc"),
									&vlQueryList{
										vlQueryLiteral("xyz"),
										vlQueryLiteral("abc.xyz"),
									},
								},
							},
						},
					},
				},
			},
			output: []string{
				"test.{foo1,foo2}.test.{foo1,foo2}",
				"test.{foo1,foo2}.test.sub.foo1",
				"test.{foo1,foo2}.test.sub.foo2.xyz{abc,xyz}",
				"test.{foo1,foo2}.test.sub.foo2.xyzxyz.abc",
				"test.{foo1,foo2}.test.sub.foo2.xyzabc.xyz",

				"test.sub.foo1.test.{foo1,foo2}",
				"test.sub.foo1.test.sub.foo1",
				"test.sub.foo1.test.sub.foo2.xyz{abc,xyz}",
				"test.sub.foo1.test.sub.foo2.xyzxyz.abc",
				"test.sub.foo1.test.sub.foo2.xyzabc.xyz",

				"test.sub.foo2.xyz{abc,xyz}.test.{foo1,foo2}",
				"test.sub.foo2.xyz{abc,xyz}.test.sub.foo1",
				"test.sub.foo2.xyz{abc,xyz}.test.sub.foo2.xyz{abc,xyz}",
				"test.sub.foo2.xyz{abc,xyz}.test.sub.foo2.xyzxyz.abc",
				"test.sub.foo2.xyz{abc,xyz}.test.sub.foo2.xyzabc.xyz",

				"test.sub.foo2.xyzxyz.abc.test.{foo1,foo2}",
				"test.sub.foo2.xyzxyz.abc.test.sub.foo1",
				"test.sub.foo2.xyzxyz.abc.test.sub.foo2.xyz{abc,xyz}",
				"test.sub.foo2.xyzxyz.abc.test.sub.foo2.xyzxyz.abc",
				"test.sub.foo2.xyzxyz.abc.test.sub.foo2.xyzabc.xyz",

				"test.sub.foo2.xyzabc.xyz.test.{foo1,foo2}",
				"test.sub.foo2.xyzabc.xyz.test.sub.foo1",
				"test.sub.foo2.xyzabc.xyz.test.sub.foo2.xyz{abc,xyz}",
				"test.sub.foo2.xyzabc.xyz.test.sub.foo2.xyzxyz.abc",
				"test.sub.foo2.xyzabc.xyz.test.sub.foo2.xyzabc.xyz",
			},
		},
		{
			name: `{{xyz0,abc0.xdb}{{xyz1,abc1,xyz1.abc}test2{abc.abc3,xyz3},}{{xyz4,abc.xyz4}{abc5,xyz5},{abc6,xyz.abc6}{{abc.xyz7,abc7}{abc.xyz8},{{{abc.xyz9}{abc.xyz10,abc10,xyz10}{abc11,xyz11,abc.xyz11}{abc12,xyz12,abc12},{abc13,xyz13}{abc14,xyz14,abc14}}}}},test15}.test16.{abc17,xyz17,abc18.{xyz19,abc19}}`,
			query: &vlQuery{
				&vlQueryList{
					&vlQuery{
						&vlQueryList{
							vlQueryLiteral("xyz0"),
							vlQueryLiteral("abc0.xdb"),
						},
						&vlQueryList{
							&vlQuery{
								&vlQueryList{
									vlQueryLiteral("xyz1"),
									vlQueryLiteral("abc1"),
									vlQueryLiteral("xyz1.abc"),
								},
								vlQueryLiteral("test2"),
								&vlQueryList{
									vlQueryLiteral("abc.abc3"),
									vlQueryLiteral("xyz3"),
								},
							},
							vlQueryLiteral(""),
						},
						&vlQueryList{
							&vlQuery{
								&vlQueryList{
									vlQueryLiteral("xyz4"),
									vlQueryLiteral("abc.xyz4"),
								},
								&vlQueryList{
									vlQueryLiteral("abc5"),
									vlQueryLiteral("xyz5"),
								},
							},
							&vlQuery{
								&vlQueryList{
									vlQueryLiteral("abc6"),
									vlQueryLiteral("xyz.abc6"),
								},
								&vlQueryList{
									&vlQuery{
										&vlQueryList{
											vlQueryLiteral("abc.xyz7"),
											vlQueryLiteral("abc7"),
										},
										&vlQueryList{
											vlQueryLiteral("abc.xyz8"),
										},
									},
									&vlQueryList{
										&vlQueryList{
											&vlQuery{
												&vlQueryList{
													vlQueryLiteral("abc.xyz9"),
												},
												&vlQueryList{
													vlQueryLiteral("abc.xyz10"),
													vlQueryLiteral("abc10"),
													vlQueryLiteral("xyz10"),
												},
												&vlQueryList{
													vlQueryLiteral("abc11"),
													vlQueryLiteral("xyz11"),
													vlQueryLiteral("abc.xyz11"),
												},
												&vlQueryList{
													vlQueryLiteral("abc12"),
													vlQueryLiteral("xyz12"),
													vlQueryLiteral("abc12"),
												},
											},
											&vlQuery{
												&vlQueryList{
													vlQueryLiteral("abc13"),
													vlQueryLiteral("xyz13"),
												},
												&vlQueryList{
													vlQueryLiteral("abc14"),
													vlQueryLiteral("xyz14"),
													vlQueryLiteral("abc14"),
												},
											},
										},
									},
								},
							},
						},
					},
					vlQueryLiteral("test15"),
				},
				vlQueryLiteral(".test16."),
				&vlQueryList{
					vlQueryLiteral("abc17"),
					vlQueryLiteral("xyz17"),
					&vlQuery{
						vlQueryLiteral("abc18."),
						&vlQueryList{
							vlQueryLiteral("xyz19"),
							vlQueryLiteral("abc19"),
						},
					},
				},
			},
			output: vlqRewriteVeryBigOutput0,
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			t.Logf("case: TestValueListQueryRewrite/'^%s$'", regexp.QuoteMeta(tcase.name))

			// log.Printf("tcase.query.String() = %+v\n", tcase.query.String())

			newQueries, err := tcase.query.expandBySubquery(65536)
			if err != nil {
				t.Error(err)
			}

			var result []string
			for _, q := range newQueries {
				result = append(result, q.String())
			}
			// pretty.Println(result)

			if diff := cmp.Diff(result, tcase.output); diff != "" {
				t.Errorf("diff: %s", diff)
			}
		})
	}
}

var vlqRewriteVeryBigOutput0 = []string{"test15.test16.{abc17,xyz17}", "test15.test16.abc18.{xyz19,abc19}", "xyz0xyz4{abc5,xyz5}.test16.{abc17,xyz17}", "xyz0xyz4{abc5,xyz5}.test16.abc18.{xyz19,abc19}", "xyz0abc.xyz4{abc5,xyz5}.test16.{abc17,xyz17}", "xyz0abc.xyz4{abc5,xyz5}.test16.abc18.{xyz19,abc19}", "xyz0abc6abc.xyz9{abc10,xyz10}{abc11,xyz11}{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "xyz0abc6abc.xyz9{abc10,xyz10}{abc11,xyz11}{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "xyz0abc6abc.xyz9{abc10,xyz10}abc.xyz11{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "xyz0abc6abc.xyz9{abc10,xyz10}abc.xyz11{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "xyz0abc6abc.xyz9abc.xyz10{abc11,xyz11}{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "xyz0abc6abc.xyz9abc.xyz10{abc11,xyz11}{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "xyz0abc6abc.xyz9abc.xyz10abc.xyz11{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "xyz0abc6abc.xyz9abc.xyz10abc.xyz11{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "xyz0abc6{abc13,xyz13}{abc14,xyz14,abc14}.test16.{abc17,xyz17}", "xyz0abc6{abc13,xyz13}{abc14,xyz14,abc14}.test16.abc18.{xyz19,abc19}", "xyz0abc6abc7abc.xyz8.test16.{abc17,xyz17}", "xyz0abc6abc7abc.xyz8.test16.abc18.{xyz19,abc19}", "xyz0abc6abc.xyz7abc.xyz8.test16.{abc17,xyz17}", "xyz0abc6abc.xyz7abc.xyz8.test16.abc18.{xyz19,abc19}", "xyz0xyz.abc6abc.xyz9{abc10,xyz10}{abc11,xyz11}{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "xyz0xyz.abc6abc.xyz9{abc10,xyz10}{abc11,xyz11}{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "xyz0xyz.abc6abc.xyz9{abc10,xyz10}abc.xyz11{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "xyz0xyz.abc6abc.xyz9{abc10,xyz10}abc.xyz11{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "xyz0xyz.abc6abc.xyz9abc.xyz10{abc11,xyz11}{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "xyz0xyz.abc6abc.xyz9abc.xyz10{abc11,xyz11}{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "xyz0xyz.abc6abc.xyz9abc.xyz10abc.xyz11{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "xyz0xyz.abc6abc.xyz9abc.xyz10abc.xyz11{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "xyz0xyz.abc6{abc13,xyz13}{abc14,xyz14,abc14}.test16.{abc17,xyz17}", "xyz0xyz.abc6{abc13,xyz13}{abc14,xyz14,abc14}.test16.abc18.{xyz19,abc19}", "xyz0xyz.abc6abc7abc.xyz8.test16.{abc17,xyz17}", "xyz0xyz.abc6abc7abc.xyz8.test16.abc18.{xyz19,abc19}", "xyz0xyz.abc6abc.xyz7abc.xyz8.test16.{abc17,xyz17}", "xyz0xyz.abc6abc.xyz7abc.xyz8.test16.abc18.{xyz19,abc19}", "xyz0{xyz1,abc1}test2xyz3xyz4{abc5,xyz5}.test16.{abc17,xyz17}", "xyz0{xyz1,abc1}test2xyz3xyz4{abc5,xyz5}.test16.abc18.{xyz19,abc19}", "xyz0{xyz1,abc1}test2xyz3abc.xyz4{abc5,xyz5}.test16.{abc17,xyz17}", "xyz0{xyz1,abc1}test2xyz3abc.xyz4{abc5,xyz5}.test16.abc18.{xyz19,abc19}", "xyz0{xyz1,abc1}test2xyz3abc6abc.xyz9{abc10,xyz10}{abc11,xyz11}{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "xyz0{xyz1,abc1}test2xyz3abc6abc.xyz9{abc10,xyz10}{abc11,xyz11}{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "xyz0{xyz1,abc1}test2xyz3abc6abc.xyz9{abc10,xyz10}abc.xyz11{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "xyz0{xyz1,abc1}test2xyz3abc6abc.xyz9{abc10,xyz10}abc.xyz11{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "xyz0{xyz1,abc1}test2xyz3abc6abc.xyz9abc.xyz10{abc11,xyz11}{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "xyz0{xyz1,abc1}test2xyz3abc6abc.xyz9abc.xyz10{abc11,xyz11}{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "xyz0{xyz1,abc1}test2xyz3abc6abc.xyz9abc.xyz10abc.xyz11{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "xyz0{xyz1,abc1}test2xyz3abc6abc.xyz9abc.xyz10abc.xyz11{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "xyz0{xyz1,abc1}test2xyz3abc6{abc13,xyz13}{abc14,xyz14,abc14}.test16.{abc17,xyz17}", "xyz0{xyz1,abc1}test2xyz3abc6{abc13,xyz13}{abc14,xyz14,abc14}.test16.abc18.{xyz19,abc19}", "xyz0{xyz1,abc1}test2xyz3abc6abc7abc.xyz8.test16.{abc17,xyz17}", "xyz0{xyz1,abc1}test2xyz3abc6abc7abc.xyz8.test16.abc18.{xyz19,abc19}", "xyz0{xyz1,abc1}test2xyz3abc6abc.xyz7abc.xyz8.test16.{abc17,xyz17}", "xyz0{xyz1,abc1}test2xyz3abc6abc.xyz7abc.xyz8.test16.abc18.{xyz19,abc19}", "xyz0{xyz1,abc1}test2xyz3xyz.abc6abc.xyz9{abc10,xyz10}{abc11,xyz11}{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "xyz0{xyz1,abc1}test2xyz3xyz.abc6abc.xyz9{abc10,xyz10}{abc11,xyz11}{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "xyz0{xyz1,abc1}test2xyz3xyz.abc6abc.xyz9{abc10,xyz10}abc.xyz11{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "xyz0{xyz1,abc1}test2xyz3xyz.abc6abc.xyz9{abc10,xyz10}abc.xyz11{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "xyz0{xyz1,abc1}test2xyz3xyz.abc6abc.xyz9abc.xyz10{abc11,xyz11}{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "xyz0{xyz1,abc1}test2xyz3xyz.abc6abc.xyz9abc.xyz10{abc11,xyz11}{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "xyz0{xyz1,abc1}test2xyz3xyz.abc6abc.xyz9abc.xyz10abc.xyz11{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "xyz0{xyz1,abc1}test2xyz3xyz.abc6abc.xyz9abc.xyz10abc.xyz11{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "xyz0{xyz1,abc1}test2xyz3xyz.abc6{abc13,xyz13}{abc14,xyz14,abc14}.test16.{abc17,xyz17}", "xyz0{xyz1,abc1}test2xyz3xyz.abc6{abc13,xyz13}{abc14,xyz14,abc14}.test16.abc18.{xyz19,abc19}", "xyz0{xyz1,abc1}test2xyz3xyz.abc6abc7abc.xyz8.test16.{abc17,xyz17}", "xyz0{xyz1,abc1}test2xyz3xyz.abc6abc7abc.xyz8.test16.abc18.{xyz19,abc19}", "xyz0{xyz1,abc1}test2xyz3xyz.abc6abc.xyz7abc.xyz8.test16.{abc17,xyz17}", "xyz0{xyz1,abc1}test2xyz3xyz.abc6abc.xyz7abc.xyz8.test16.abc18.{xyz19,abc19}", "xyz0{xyz1,abc1}test2abc.abc3xyz4{abc5,xyz5}.test16.{abc17,xyz17}", "xyz0{xyz1,abc1}test2abc.abc3xyz4{abc5,xyz5}.test16.abc18.{xyz19,abc19}", "xyz0{xyz1,abc1}test2abc.abc3abc.xyz4{abc5,xyz5}.test16.{abc17,xyz17}", "xyz0{xyz1,abc1}test2abc.abc3abc.xyz4{abc5,xyz5}.test16.abc18.{xyz19,abc19}", "xyz0{xyz1,abc1}test2abc.abc3abc6abc.xyz9{abc10,xyz10}{abc11,xyz11}{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "xyz0{xyz1,abc1}test2abc.abc3abc6abc.xyz9{abc10,xyz10}{abc11,xyz11}{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "xyz0{xyz1,abc1}test2abc.abc3abc6abc.xyz9{abc10,xyz10}abc.xyz11{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "xyz0{xyz1,abc1}test2abc.abc3abc6abc.xyz9{abc10,xyz10}abc.xyz11{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "xyz0{xyz1,abc1}test2abc.abc3abc6abc.xyz9abc.xyz10{abc11,xyz11}{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "xyz0{xyz1,abc1}test2abc.abc3abc6abc.xyz9abc.xyz10{abc11,xyz11}{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "xyz0{xyz1,abc1}test2abc.abc3abc6abc.xyz9abc.xyz10abc.xyz11{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "xyz0{xyz1,abc1}test2abc.abc3abc6abc.xyz9abc.xyz10abc.xyz11{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "xyz0{xyz1,abc1}test2abc.abc3abc6{abc13,xyz13}{abc14,xyz14,abc14}.test16.{abc17,xyz17}", "xyz0{xyz1,abc1}test2abc.abc3abc6{abc13,xyz13}{abc14,xyz14,abc14}.test16.abc18.{xyz19,abc19}", "xyz0{xyz1,abc1}test2abc.abc3abc6abc7abc.xyz8.test16.{abc17,xyz17}", "xyz0{xyz1,abc1}test2abc.abc3abc6abc7abc.xyz8.test16.abc18.{xyz19,abc19}", "xyz0{xyz1,abc1}test2abc.abc3abc6abc.xyz7abc.xyz8.test16.{abc17,xyz17}", "xyz0{xyz1,abc1}test2abc.abc3abc6abc.xyz7abc.xyz8.test16.abc18.{xyz19,abc19}", "xyz0{xyz1,abc1}test2abc.abc3xyz.abc6abc.xyz9{abc10,xyz10}{abc11,xyz11}{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "xyz0{xyz1,abc1}test2abc.abc3xyz.abc6abc.xyz9{abc10,xyz10}{abc11,xyz11}{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "xyz0{xyz1,abc1}test2abc.abc3xyz.abc6abc.xyz9{abc10,xyz10}abc.xyz11{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "xyz0{xyz1,abc1}test2abc.abc3xyz.abc6abc.xyz9{abc10,xyz10}abc.xyz11{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "xyz0{xyz1,abc1}test2abc.abc3xyz.abc6abc.xyz9abc.xyz10{abc11,xyz11}{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "xyz0{xyz1,abc1}test2abc.abc3xyz.abc6abc.xyz9abc.xyz10{abc11,xyz11}{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "xyz0{xyz1,abc1}test2abc.abc3xyz.abc6abc.xyz9abc.xyz10abc.xyz11{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "xyz0{xyz1,abc1}test2abc.abc3xyz.abc6abc.xyz9abc.xyz10abc.xyz11{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "xyz0{xyz1,abc1}test2abc.abc3xyz.abc6{abc13,xyz13}{abc14,xyz14,abc14}.test16.{abc17,xyz17}", "xyz0{xyz1,abc1}test2abc.abc3xyz.abc6{abc13,xyz13}{abc14,xyz14,abc14}.test16.abc18.{xyz19,abc19}", "xyz0{xyz1,abc1}test2abc.abc3xyz.abc6abc7abc.xyz8.test16.{abc17,xyz17}", "xyz0{xyz1,abc1}test2abc.abc3xyz.abc6abc7abc.xyz8.test16.abc18.{xyz19,abc19}", "xyz0{xyz1,abc1}test2abc.abc3xyz.abc6abc.xyz7abc.xyz8.test16.{abc17,xyz17}", "xyz0{xyz1,abc1}test2abc.abc3xyz.abc6abc.xyz7abc.xyz8.test16.abc18.{xyz19,abc19}", "xyz0xyz1.abctest2xyz3xyz4{abc5,xyz5}.test16.{abc17,xyz17}", "xyz0xyz1.abctest2xyz3xyz4{abc5,xyz5}.test16.abc18.{xyz19,abc19}", "xyz0xyz1.abctest2xyz3abc.xyz4{abc5,xyz5}.test16.{abc17,xyz17}", "xyz0xyz1.abctest2xyz3abc.xyz4{abc5,xyz5}.test16.abc18.{xyz19,abc19}", "xyz0xyz1.abctest2xyz3abc6abc.xyz9{abc10,xyz10}{abc11,xyz11}{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "xyz0xyz1.abctest2xyz3abc6abc.xyz9{abc10,xyz10}{abc11,xyz11}{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "xyz0xyz1.abctest2xyz3abc6abc.xyz9{abc10,xyz10}abc.xyz11{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "xyz0xyz1.abctest2xyz3abc6abc.xyz9{abc10,xyz10}abc.xyz11{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "xyz0xyz1.abctest2xyz3abc6abc.xyz9abc.xyz10{abc11,xyz11}{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "xyz0xyz1.abctest2xyz3abc6abc.xyz9abc.xyz10{abc11,xyz11}{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "xyz0xyz1.abctest2xyz3abc6abc.xyz9abc.xyz10abc.xyz11{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "xyz0xyz1.abctest2xyz3abc6abc.xyz9abc.xyz10abc.xyz11{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "xyz0xyz1.abctest2xyz3abc6{abc13,xyz13}{abc14,xyz14,abc14}.test16.{abc17,xyz17}", "xyz0xyz1.abctest2xyz3abc6{abc13,xyz13}{abc14,xyz14,abc14}.test16.abc18.{xyz19,abc19}", "xyz0xyz1.abctest2xyz3abc6abc7abc.xyz8.test16.{abc17,xyz17}", "xyz0xyz1.abctest2xyz3abc6abc7abc.xyz8.test16.abc18.{xyz19,abc19}", "xyz0xyz1.abctest2xyz3abc6abc.xyz7abc.xyz8.test16.{abc17,xyz17}", "xyz0xyz1.abctest2xyz3abc6abc.xyz7abc.xyz8.test16.abc18.{xyz19,abc19}", "xyz0xyz1.abctest2xyz3xyz.abc6abc.xyz9{abc10,xyz10}{abc11,xyz11}{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "xyz0xyz1.abctest2xyz3xyz.abc6abc.xyz9{abc10,xyz10}{abc11,xyz11}{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "xyz0xyz1.abctest2xyz3xyz.abc6abc.xyz9{abc10,xyz10}abc.xyz11{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "xyz0xyz1.abctest2xyz3xyz.abc6abc.xyz9{abc10,xyz10}abc.xyz11{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "xyz0xyz1.abctest2xyz3xyz.abc6abc.xyz9abc.xyz10{abc11,xyz11}{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "xyz0xyz1.abctest2xyz3xyz.abc6abc.xyz9abc.xyz10{abc11,xyz11}{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "xyz0xyz1.abctest2xyz3xyz.abc6abc.xyz9abc.xyz10abc.xyz11{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "xyz0xyz1.abctest2xyz3xyz.abc6abc.xyz9abc.xyz10abc.xyz11{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "xyz0xyz1.abctest2xyz3xyz.abc6{abc13,xyz13}{abc14,xyz14,abc14}.test16.{abc17,xyz17}", "xyz0xyz1.abctest2xyz3xyz.abc6{abc13,xyz13}{abc14,xyz14,abc14}.test16.abc18.{xyz19,abc19}", "xyz0xyz1.abctest2xyz3xyz.abc6abc7abc.xyz8.test16.{abc17,xyz17}", "xyz0xyz1.abctest2xyz3xyz.abc6abc7abc.xyz8.test16.abc18.{xyz19,abc19}", "xyz0xyz1.abctest2xyz3xyz.abc6abc.xyz7abc.xyz8.test16.{abc17,xyz17}", "xyz0xyz1.abctest2xyz3xyz.abc6abc.xyz7abc.xyz8.test16.abc18.{xyz19,abc19}", "xyz0xyz1.abctest2abc.abc3xyz4{abc5,xyz5}.test16.{abc17,xyz17}", "xyz0xyz1.abctest2abc.abc3xyz4{abc5,xyz5}.test16.abc18.{xyz19,abc19}", "xyz0xyz1.abctest2abc.abc3abc.xyz4{abc5,xyz5}.test16.{abc17,xyz17}", "xyz0xyz1.abctest2abc.abc3abc.xyz4{abc5,xyz5}.test16.abc18.{xyz19,abc19}", "xyz0xyz1.abctest2abc.abc3abc6abc.xyz9{abc10,xyz10}{abc11,xyz11}{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "xyz0xyz1.abctest2abc.abc3abc6abc.xyz9{abc10,xyz10}{abc11,xyz11}{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "xyz0xyz1.abctest2abc.abc3abc6abc.xyz9{abc10,xyz10}abc.xyz11{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "xyz0xyz1.abctest2abc.abc3abc6abc.xyz9{abc10,xyz10}abc.xyz11{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "xyz0xyz1.abctest2abc.abc3abc6abc.xyz9abc.xyz10{abc11,xyz11}{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "xyz0xyz1.abctest2abc.abc3abc6abc.xyz9abc.xyz10{abc11,xyz11}{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "xyz0xyz1.abctest2abc.abc3abc6abc.xyz9abc.xyz10abc.xyz11{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "xyz0xyz1.abctest2abc.abc3abc6abc.xyz9abc.xyz10abc.xyz11{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "xyz0xyz1.abctest2abc.abc3abc6{abc13,xyz13}{abc14,xyz14,abc14}.test16.{abc17,xyz17}", "xyz0xyz1.abctest2abc.abc3abc6{abc13,xyz13}{abc14,xyz14,abc14}.test16.abc18.{xyz19,abc19}", "xyz0xyz1.abctest2abc.abc3abc6abc7abc.xyz8.test16.{abc17,xyz17}", "xyz0xyz1.abctest2abc.abc3abc6abc7abc.xyz8.test16.abc18.{xyz19,abc19}", "xyz0xyz1.abctest2abc.abc3abc6abc.xyz7abc.xyz8.test16.{abc17,xyz17}", "xyz0xyz1.abctest2abc.abc3abc6abc.xyz7abc.xyz8.test16.abc18.{xyz19,abc19}", "xyz0xyz1.abctest2abc.abc3xyz.abc6abc.xyz9{abc10,xyz10}{abc11,xyz11}{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "xyz0xyz1.abctest2abc.abc3xyz.abc6abc.xyz9{abc10,xyz10}{abc11,xyz11}{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "xyz0xyz1.abctest2abc.abc3xyz.abc6abc.xyz9{abc10,xyz10}abc.xyz11{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "xyz0xyz1.abctest2abc.abc3xyz.abc6abc.xyz9{abc10,xyz10}abc.xyz11{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "xyz0xyz1.abctest2abc.abc3xyz.abc6abc.xyz9abc.xyz10{abc11,xyz11}{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "xyz0xyz1.abctest2abc.abc3xyz.abc6abc.xyz9abc.xyz10{abc11,xyz11}{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "xyz0xyz1.abctest2abc.abc3xyz.abc6abc.xyz9abc.xyz10abc.xyz11{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "xyz0xyz1.abctest2abc.abc3xyz.abc6abc.xyz9abc.xyz10abc.xyz11{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "xyz0xyz1.abctest2abc.abc3xyz.abc6{abc13,xyz13}{abc14,xyz14,abc14}.test16.{abc17,xyz17}", "xyz0xyz1.abctest2abc.abc3xyz.abc6{abc13,xyz13}{abc14,xyz14,abc14}.test16.abc18.{xyz19,abc19}", "xyz0xyz1.abctest2abc.abc3xyz.abc6abc7abc.xyz8.test16.{abc17,xyz17}", "xyz0xyz1.abctest2abc.abc3xyz.abc6abc7abc.xyz8.test16.abc18.{xyz19,abc19}", "xyz0xyz1.abctest2abc.abc3xyz.abc6abc.xyz7abc.xyz8.test16.{abc17,xyz17}", "xyz0xyz1.abctest2abc.abc3xyz.abc6abc.xyz7abc.xyz8.test16.abc18.{xyz19,abc19}", "abc0.xdbxyz4{abc5,xyz5}.test16.{abc17,xyz17}", "abc0.xdbxyz4{abc5,xyz5}.test16.abc18.{xyz19,abc19}", "abc0.xdbabc.xyz4{abc5,xyz5}.test16.{abc17,xyz17}", "abc0.xdbabc.xyz4{abc5,xyz5}.test16.abc18.{xyz19,abc19}", "abc0.xdbabc6abc.xyz9{abc10,xyz10}{abc11,xyz11}{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "abc0.xdbabc6abc.xyz9{abc10,xyz10}{abc11,xyz11}{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "abc0.xdbabc6abc.xyz9{abc10,xyz10}abc.xyz11{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "abc0.xdbabc6abc.xyz9{abc10,xyz10}abc.xyz11{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "abc0.xdbabc6abc.xyz9abc.xyz10{abc11,xyz11}{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "abc0.xdbabc6abc.xyz9abc.xyz10{abc11,xyz11}{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "abc0.xdbabc6abc.xyz9abc.xyz10abc.xyz11{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "abc0.xdbabc6abc.xyz9abc.xyz10abc.xyz11{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "abc0.xdbabc6{abc13,xyz13}{abc14,xyz14,abc14}.test16.{abc17,xyz17}", "abc0.xdbabc6{abc13,xyz13}{abc14,xyz14,abc14}.test16.abc18.{xyz19,abc19}", "abc0.xdbabc6abc7abc.xyz8.test16.{abc17,xyz17}", "abc0.xdbabc6abc7abc.xyz8.test16.abc18.{xyz19,abc19}", "abc0.xdbabc6abc.xyz7abc.xyz8.test16.{abc17,xyz17}", "abc0.xdbabc6abc.xyz7abc.xyz8.test16.abc18.{xyz19,abc19}", "abc0.xdbxyz.abc6abc.xyz9{abc10,xyz10}{abc11,xyz11}{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "abc0.xdbxyz.abc6abc.xyz9{abc10,xyz10}{abc11,xyz11}{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "abc0.xdbxyz.abc6abc.xyz9{abc10,xyz10}abc.xyz11{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "abc0.xdbxyz.abc6abc.xyz9{abc10,xyz10}abc.xyz11{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "abc0.xdbxyz.abc6abc.xyz9abc.xyz10{abc11,xyz11}{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "abc0.xdbxyz.abc6abc.xyz9abc.xyz10{abc11,xyz11}{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "abc0.xdbxyz.abc6abc.xyz9abc.xyz10abc.xyz11{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "abc0.xdbxyz.abc6abc.xyz9abc.xyz10abc.xyz11{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "abc0.xdbxyz.abc6{abc13,xyz13}{abc14,xyz14,abc14}.test16.{abc17,xyz17}", "abc0.xdbxyz.abc6{abc13,xyz13}{abc14,xyz14,abc14}.test16.abc18.{xyz19,abc19}", "abc0.xdbxyz.abc6abc7abc.xyz8.test16.{abc17,xyz17}", "abc0.xdbxyz.abc6abc7abc.xyz8.test16.abc18.{xyz19,abc19}", "abc0.xdbxyz.abc6abc.xyz7abc.xyz8.test16.{abc17,xyz17}", "abc0.xdbxyz.abc6abc.xyz7abc.xyz8.test16.abc18.{xyz19,abc19}", "abc0.xdb{xyz1,abc1}test2xyz3xyz4{abc5,xyz5}.test16.{abc17,xyz17}", "abc0.xdb{xyz1,abc1}test2xyz3xyz4{abc5,xyz5}.test16.abc18.{xyz19,abc19}", "abc0.xdb{xyz1,abc1}test2xyz3abc.xyz4{abc5,xyz5}.test16.{abc17,xyz17}", "abc0.xdb{xyz1,abc1}test2xyz3abc.xyz4{abc5,xyz5}.test16.abc18.{xyz19,abc19}", "abc0.xdb{xyz1,abc1}test2xyz3abc6abc.xyz9{abc10,xyz10}{abc11,xyz11}{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "abc0.xdb{xyz1,abc1}test2xyz3abc6abc.xyz9{abc10,xyz10}{abc11,xyz11}{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "abc0.xdb{xyz1,abc1}test2xyz3abc6abc.xyz9{abc10,xyz10}abc.xyz11{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "abc0.xdb{xyz1,abc1}test2xyz3abc6abc.xyz9{abc10,xyz10}abc.xyz11{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "abc0.xdb{xyz1,abc1}test2xyz3abc6abc.xyz9abc.xyz10{abc11,xyz11}{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "abc0.xdb{xyz1,abc1}test2xyz3abc6abc.xyz9abc.xyz10{abc11,xyz11}{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "abc0.xdb{xyz1,abc1}test2xyz3abc6abc.xyz9abc.xyz10abc.xyz11{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "abc0.xdb{xyz1,abc1}test2xyz3abc6abc.xyz9abc.xyz10abc.xyz11{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "abc0.xdb{xyz1,abc1}test2xyz3abc6{abc13,xyz13}{abc14,xyz14,abc14}.test16.{abc17,xyz17}", "abc0.xdb{xyz1,abc1}test2xyz3abc6{abc13,xyz13}{abc14,xyz14,abc14}.test16.abc18.{xyz19,abc19}", "abc0.xdb{xyz1,abc1}test2xyz3abc6abc7abc.xyz8.test16.{abc17,xyz17}", "abc0.xdb{xyz1,abc1}test2xyz3abc6abc7abc.xyz8.test16.abc18.{xyz19,abc19}", "abc0.xdb{xyz1,abc1}test2xyz3abc6abc.xyz7abc.xyz8.test16.{abc17,xyz17}", "abc0.xdb{xyz1,abc1}test2xyz3abc6abc.xyz7abc.xyz8.test16.abc18.{xyz19,abc19}", "abc0.xdb{xyz1,abc1}test2xyz3xyz.abc6abc.xyz9{abc10,xyz10}{abc11,xyz11}{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "abc0.xdb{xyz1,abc1}test2xyz3xyz.abc6abc.xyz9{abc10,xyz10}{abc11,xyz11}{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "abc0.xdb{xyz1,abc1}test2xyz3xyz.abc6abc.xyz9{abc10,xyz10}abc.xyz11{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "abc0.xdb{xyz1,abc1}test2xyz3xyz.abc6abc.xyz9{abc10,xyz10}abc.xyz11{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "abc0.xdb{xyz1,abc1}test2xyz3xyz.abc6abc.xyz9abc.xyz10{abc11,xyz11}{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "abc0.xdb{xyz1,abc1}test2xyz3xyz.abc6abc.xyz9abc.xyz10{abc11,xyz11}{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "abc0.xdb{xyz1,abc1}test2xyz3xyz.abc6abc.xyz9abc.xyz10abc.xyz11{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "abc0.xdb{xyz1,abc1}test2xyz3xyz.abc6abc.xyz9abc.xyz10abc.xyz11{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "abc0.xdb{xyz1,abc1}test2xyz3xyz.abc6{abc13,xyz13}{abc14,xyz14,abc14}.test16.{abc17,xyz17}", "abc0.xdb{xyz1,abc1}test2xyz3xyz.abc6{abc13,xyz13}{abc14,xyz14,abc14}.test16.abc18.{xyz19,abc19}", "abc0.xdb{xyz1,abc1}test2xyz3xyz.abc6abc7abc.xyz8.test16.{abc17,xyz17}", "abc0.xdb{xyz1,abc1}test2xyz3xyz.abc6abc7abc.xyz8.test16.abc18.{xyz19,abc19}", "abc0.xdb{xyz1,abc1}test2xyz3xyz.abc6abc.xyz7abc.xyz8.test16.{abc17,xyz17}", "abc0.xdb{xyz1,abc1}test2xyz3xyz.abc6abc.xyz7abc.xyz8.test16.abc18.{xyz19,abc19}", "abc0.xdb{xyz1,abc1}test2abc.abc3xyz4{abc5,xyz5}.test16.{abc17,xyz17}", "abc0.xdb{xyz1,abc1}test2abc.abc3xyz4{abc5,xyz5}.test16.abc18.{xyz19,abc19}", "abc0.xdb{xyz1,abc1}test2abc.abc3abc.xyz4{abc5,xyz5}.test16.{abc17,xyz17}", "abc0.xdb{xyz1,abc1}test2abc.abc3abc.xyz4{abc5,xyz5}.test16.abc18.{xyz19,abc19}", "abc0.xdb{xyz1,abc1}test2abc.abc3abc6abc.xyz9{abc10,xyz10}{abc11,xyz11}{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "abc0.xdb{xyz1,abc1}test2abc.abc3abc6abc.xyz9{abc10,xyz10}{abc11,xyz11}{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "abc0.xdb{xyz1,abc1}test2abc.abc3abc6abc.xyz9{abc10,xyz10}abc.xyz11{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "abc0.xdb{xyz1,abc1}test2abc.abc3abc6abc.xyz9{abc10,xyz10}abc.xyz11{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "abc0.xdb{xyz1,abc1}test2abc.abc3abc6abc.xyz9abc.xyz10{abc11,xyz11}{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "abc0.xdb{xyz1,abc1}test2abc.abc3abc6abc.xyz9abc.xyz10{abc11,xyz11}{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "abc0.xdb{xyz1,abc1}test2abc.abc3abc6abc.xyz9abc.xyz10abc.xyz11{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "abc0.xdb{xyz1,abc1}test2abc.abc3abc6abc.xyz9abc.xyz10abc.xyz11{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "abc0.xdb{xyz1,abc1}test2abc.abc3abc6{abc13,xyz13}{abc14,xyz14,abc14}.test16.{abc17,xyz17}", "abc0.xdb{xyz1,abc1}test2abc.abc3abc6{abc13,xyz13}{abc14,xyz14,abc14}.test16.abc18.{xyz19,abc19}", "abc0.xdb{xyz1,abc1}test2abc.abc3abc6abc7abc.xyz8.test16.{abc17,xyz17}", "abc0.xdb{xyz1,abc1}test2abc.abc3abc6abc7abc.xyz8.test16.abc18.{xyz19,abc19}", "abc0.xdb{xyz1,abc1}test2abc.abc3abc6abc.xyz7abc.xyz8.test16.{abc17,xyz17}", "abc0.xdb{xyz1,abc1}test2abc.abc3abc6abc.xyz7abc.xyz8.test16.abc18.{xyz19,abc19}", "abc0.xdb{xyz1,abc1}test2abc.abc3xyz.abc6abc.xyz9{abc10,xyz10}{abc11,xyz11}{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "abc0.xdb{xyz1,abc1}test2abc.abc3xyz.abc6abc.xyz9{abc10,xyz10}{abc11,xyz11}{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "abc0.xdb{xyz1,abc1}test2abc.abc3xyz.abc6abc.xyz9{abc10,xyz10}abc.xyz11{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "abc0.xdb{xyz1,abc1}test2abc.abc3xyz.abc6abc.xyz9{abc10,xyz10}abc.xyz11{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "abc0.xdb{xyz1,abc1}test2abc.abc3xyz.abc6abc.xyz9abc.xyz10{abc11,xyz11}{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "abc0.xdb{xyz1,abc1}test2abc.abc3xyz.abc6abc.xyz9abc.xyz10{abc11,xyz11}{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "abc0.xdb{xyz1,abc1}test2abc.abc3xyz.abc6abc.xyz9abc.xyz10abc.xyz11{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "abc0.xdb{xyz1,abc1}test2abc.abc3xyz.abc6abc.xyz9abc.xyz10abc.xyz11{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "abc0.xdb{xyz1,abc1}test2abc.abc3xyz.abc6{abc13,xyz13}{abc14,xyz14,abc14}.test16.{abc17,xyz17}", "abc0.xdb{xyz1,abc1}test2abc.abc3xyz.abc6{abc13,xyz13}{abc14,xyz14,abc14}.test16.abc18.{xyz19,abc19}", "abc0.xdb{xyz1,abc1}test2abc.abc3xyz.abc6abc7abc.xyz8.test16.{abc17,xyz17}", "abc0.xdb{xyz1,abc1}test2abc.abc3xyz.abc6abc7abc.xyz8.test16.abc18.{xyz19,abc19}", "abc0.xdb{xyz1,abc1}test2abc.abc3xyz.abc6abc.xyz7abc.xyz8.test16.{abc17,xyz17}", "abc0.xdb{xyz1,abc1}test2abc.abc3xyz.abc6abc.xyz7abc.xyz8.test16.abc18.{xyz19,abc19}", "abc0.xdbxyz1.abctest2xyz3xyz4{abc5,xyz5}.test16.{abc17,xyz17}", "abc0.xdbxyz1.abctest2xyz3xyz4{abc5,xyz5}.test16.abc18.{xyz19,abc19}", "abc0.xdbxyz1.abctest2xyz3abc.xyz4{abc5,xyz5}.test16.{abc17,xyz17}", "abc0.xdbxyz1.abctest2xyz3abc.xyz4{abc5,xyz5}.test16.abc18.{xyz19,abc19}", "abc0.xdbxyz1.abctest2xyz3abc6abc.xyz9{abc10,xyz10}{abc11,xyz11}{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "abc0.xdbxyz1.abctest2xyz3abc6abc.xyz9{abc10,xyz10}{abc11,xyz11}{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "abc0.xdbxyz1.abctest2xyz3abc6abc.xyz9{abc10,xyz10}abc.xyz11{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "abc0.xdbxyz1.abctest2xyz3abc6abc.xyz9{abc10,xyz10}abc.xyz11{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "abc0.xdbxyz1.abctest2xyz3abc6abc.xyz9abc.xyz10{abc11,xyz11}{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "abc0.xdbxyz1.abctest2xyz3abc6abc.xyz9abc.xyz10{abc11,xyz11}{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "abc0.xdbxyz1.abctest2xyz3abc6abc.xyz9abc.xyz10abc.xyz11{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "abc0.xdbxyz1.abctest2xyz3abc6abc.xyz9abc.xyz10abc.xyz11{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "abc0.xdbxyz1.abctest2xyz3abc6{abc13,xyz13}{abc14,xyz14,abc14}.test16.{abc17,xyz17}", "abc0.xdbxyz1.abctest2xyz3abc6{abc13,xyz13}{abc14,xyz14,abc14}.test16.abc18.{xyz19,abc19}", "abc0.xdbxyz1.abctest2xyz3abc6abc7abc.xyz8.test16.{abc17,xyz17}", "abc0.xdbxyz1.abctest2xyz3abc6abc7abc.xyz8.test16.abc18.{xyz19,abc19}", "abc0.xdbxyz1.abctest2xyz3abc6abc.xyz7abc.xyz8.test16.{abc17,xyz17}", "abc0.xdbxyz1.abctest2xyz3abc6abc.xyz7abc.xyz8.test16.abc18.{xyz19,abc19}", "abc0.xdbxyz1.abctest2xyz3xyz.abc6abc.xyz9{abc10,xyz10}{abc11,xyz11}{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "abc0.xdbxyz1.abctest2xyz3xyz.abc6abc.xyz9{abc10,xyz10}{abc11,xyz11}{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "abc0.xdbxyz1.abctest2xyz3xyz.abc6abc.xyz9{abc10,xyz10}abc.xyz11{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "abc0.xdbxyz1.abctest2xyz3xyz.abc6abc.xyz9{abc10,xyz10}abc.xyz11{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "abc0.xdbxyz1.abctest2xyz3xyz.abc6abc.xyz9abc.xyz10{abc11,xyz11}{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "abc0.xdbxyz1.abctest2xyz3xyz.abc6abc.xyz9abc.xyz10{abc11,xyz11}{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "abc0.xdbxyz1.abctest2xyz3xyz.abc6abc.xyz9abc.xyz10abc.xyz11{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "abc0.xdbxyz1.abctest2xyz3xyz.abc6abc.xyz9abc.xyz10abc.xyz11{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "abc0.xdbxyz1.abctest2xyz3xyz.abc6{abc13,xyz13}{abc14,xyz14,abc14}.test16.{abc17,xyz17}", "abc0.xdbxyz1.abctest2xyz3xyz.abc6{abc13,xyz13}{abc14,xyz14,abc14}.test16.abc18.{xyz19,abc19}", "abc0.xdbxyz1.abctest2xyz3xyz.abc6abc7abc.xyz8.test16.{abc17,xyz17}", "abc0.xdbxyz1.abctest2xyz3xyz.abc6abc7abc.xyz8.test16.abc18.{xyz19,abc19}", "abc0.xdbxyz1.abctest2xyz3xyz.abc6abc.xyz7abc.xyz8.test16.{abc17,xyz17}", "abc0.xdbxyz1.abctest2xyz3xyz.abc6abc.xyz7abc.xyz8.test16.abc18.{xyz19,abc19}", "abc0.xdbxyz1.abctest2abc.abc3xyz4{abc5,xyz5}.test16.{abc17,xyz17}", "abc0.xdbxyz1.abctest2abc.abc3xyz4{abc5,xyz5}.test16.abc18.{xyz19,abc19}", "abc0.xdbxyz1.abctest2abc.abc3abc.xyz4{abc5,xyz5}.test16.{abc17,xyz17}", "abc0.xdbxyz1.abctest2abc.abc3abc.xyz4{abc5,xyz5}.test16.abc18.{xyz19,abc19}", "abc0.xdbxyz1.abctest2abc.abc3abc6abc.xyz9{abc10,xyz10}{abc11,xyz11}{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "abc0.xdbxyz1.abctest2abc.abc3abc6abc.xyz9{abc10,xyz10}{abc11,xyz11}{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "abc0.xdbxyz1.abctest2abc.abc3abc6abc.xyz9{abc10,xyz10}abc.xyz11{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "abc0.xdbxyz1.abctest2abc.abc3abc6abc.xyz9{abc10,xyz10}abc.xyz11{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "abc0.xdbxyz1.abctest2abc.abc3abc6abc.xyz9abc.xyz10{abc11,xyz11}{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "abc0.xdbxyz1.abctest2abc.abc3abc6abc.xyz9abc.xyz10{abc11,xyz11}{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "abc0.xdbxyz1.abctest2abc.abc3abc6abc.xyz9abc.xyz10abc.xyz11{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "abc0.xdbxyz1.abctest2abc.abc3abc6abc.xyz9abc.xyz10abc.xyz11{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "abc0.xdbxyz1.abctest2abc.abc3abc6{abc13,xyz13}{abc14,xyz14,abc14}.test16.{abc17,xyz17}", "abc0.xdbxyz1.abctest2abc.abc3abc6{abc13,xyz13}{abc14,xyz14,abc14}.test16.abc18.{xyz19,abc19}", "abc0.xdbxyz1.abctest2abc.abc3abc6abc7abc.xyz8.test16.{abc17,xyz17}", "abc0.xdbxyz1.abctest2abc.abc3abc6abc7abc.xyz8.test16.abc18.{xyz19,abc19}", "abc0.xdbxyz1.abctest2abc.abc3abc6abc.xyz7abc.xyz8.test16.{abc17,xyz17}", "abc0.xdbxyz1.abctest2abc.abc3abc6abc.xyz7abc.xyz8.test16.abc18.{xyz19,abc19}", "abc0.xdbxyz1.abctest2abc.abc3xyz.abc6abc.xyz9{abc10,xyz10}{abc11,xyz11}{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "abc0.xdbxyz1.abctest2abc.abc3xyz.abc6abc.xyz9{abc10,xyz10}{abc11,xyz11}{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "abc0.xdbxyz1.abctest2abc.abc3xyz.abc6abc.xyz9{abc10,xyz10}abc.xyz11{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "abc0.xdbxyz1.abctest2abc.abc3xyz.abc6abc.xyz9{abc10,xyz10}abc.xyz11{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "abc0.xdbxyz1.abctest2abc.abc3xyz.abc6abc.xyz9abc.xyz10{abc11,xyz11}{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "abc0.xdbxyz1.abctest2abc.abc3xyz.abc6abc.xyz9abc.xyz10{abc11,xyz11}{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "abc0.xdbxyz1.abctest2abc.abc3xyz.abc6abc.xyz9abc.xyz10abc.xyz11{abc12,xyz12,abc12}.test16.{abc17,xyz17}", "abc0.xdbxyz1.abctest2abc.abc3xyz.abc6abc.xyz9abc.xyz10abc.xyz11{abc12,xyz12,abc12}.test16.abc18.{xyz19,abc19}", "abc0.xdbxyz1.abctest2abc.abc3xyz.abc6{abc13,xyz13}{abc14,xyz14,abc14}.test16.{abc17,xyz17}", "abc0.xdbxyz1.abctest2abc.abc3xyz.abc6{abc13,xyz13}{abc14,xyz14,abc14}.test16.abc18.{xyz19,abc19}", "abc0.xdbxyz1.abctest2abc.abc3xyz.abc6abc7abc.xyz8.test16.{abc17,xyz17}", "abc0.xdbxyz1.abctest2abc.abc3xyz.abc6abc7abc.xyz8.test16.abc18.{xyz19,abc19}", "abc0.xdbxyz1.abctest2abc.abc3xyz.abc6abc.xyz7abc.xyz8.test16.{abc17,xyz17}", "abc0.xdbxyz1.abctest2abc.abc3xyz.abc6abc.xyz7abc.xyz8.test16.abc18.{xyz19,abc19}"}
