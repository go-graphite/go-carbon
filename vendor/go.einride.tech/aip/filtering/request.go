package filtering

// Request is an interface for gRPC requests that contain a standard AIP filter.
type Request interface {
	GetFilter() string
}

// ParseFilter parses and type-checks the filter in the provided Request.
func ParseFilter(request Request, declarations *Declarations) (Filter, error) {
	if request.GetFilter() == "" {
		return Filter{}, nil
	}
	var parser Parser
	parser.Init(request.GetFilter())
	parsedExpr, err := parser.Parse()
	if err != nil {
		return Filter{}, err
	}
	var checker Checker
	checker.Init(parsedExpr.GetExpr(), parsedExpr.GetSourceInfo(), declarations)
	checkedExpr, err := checker.Check()
	if err != nil {
		return Filter{}, err
	}
	return Filter{
		CheckedExpr: checkedExpr,
	}, nil
}
