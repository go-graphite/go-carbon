package carbonserver

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dgryski/go-trigram"
	"github.com/go-graphite/go-carbon/points"
	"go.uber.org/zap"
)

func init() { log.SetFlags(log.Lshortfile) }

type logf interface {
	Logf(format string, args ...interface{})
}

// skipcq: RVV-A0005
func newTrieServer(files []string, withTrigram bool, l logf) *CarbonserverListener {
	var listener CarbonserverListener
	listener.logger = zap.NewNop()
	listener.accessLogger = zap.NewNop()
	listener.trieIndex = true
	listener.whisperData = "./testdata"
	listener.maxGlobs = math.MaxInt64
	listener.maxMetricsGlobbed = math.MaxInt64
	listener.maxMetricsRendered = math.MaxInt64
	listener.failOnMaxGlobs = true

	start := time.Now()
	trieIndex := newTrie(".wsp", nil)
	for _, file := range files {
		trieIndex.insert(file, 0, 0, 0)
	}
	l.Logf("trie index took %s\n", time.Since(start))

	if withTrigram {
		start = time.Now()
		trieIndex.setTrigrams()
		l.Logf("trie setTrigrams took %s size %d\n", time.Since(start), len(trieIndex.trigrams))
	}

	l.Logf("longest metric(%d): %s\n", trieIndex.depth, trieIndex.longestMetric)

	listener.UpdateFileIndex(&fileIndex{
		trieIdx: trieIndex,
	})

	return &listener
}

func newTrigramServer(files []string, l logf) *CarbonserverListener {
	var listener CarbonserverListener
	listener.logger = zap.NewNop()
	listener.accessLogger = zap.NewNop()
	listener.trigramIndex = true
	listener.whisperData = "./testdata"
	listener.maxGlobs = math.MaxInt64
	listener.maxMetricsGlobbed = math.MaxInt64
	listener.maxMetricsRendered = math.MaxInt64
	listener.failOnMaxGlobs = true

	start := time.Now()
	idx := trigram.NewIndex(files)
	l.Logf("trigram index took %s\n", time.Since(start))

	listener.UpdateFileIndex(&fileIndex{
		idx:   idx,
		files: files,
	})

	return &listener
}

func TestTrieIndex(t *testing.T) {
	commonFiles := []string{
		"/something/else/server.wsp",
		"/service-00/server-000/metric-namespace-000/43081e003a315b88.wsp",
		"/service-00/server-000/metric-namespace-000/cpu.wsp",
		"/service-00/server-000/metric-namespace-001/d218bc1539f2cf8.wsp",
		"/service-00/server-000/metric-namespace-001/cpu.wsp",
		"/service-00/server-000/metric-namespace-005/cpu.wsp",
		"/service-00/server-000/metric-namespace-002/29370bc791c0fccb.wsp",
		"/service-00/server-000/metric-namespace-002/cpu.wsp",

		"/service-00/server-001/metric-namespace-002/cpu.wsp",
		"/service-00/server-001/metric-namespace-005/cpu.wsp",

		"/service-01/server-000/metric-namespace-004/6f31f9305c67895c.wsp",
		"/service-01/server-000/metric-namespace-004/cpu.wsp",
		"/service-01/server-000/metric-namespace-005/cpu.wsp",

		"/service-00/server-002/metric-namespace-003/64cd3228c99afc54.wsp",
		"/service-00/server-002/metric-namespace-003/cpu.wsp",
		"/service-00/server-002/metric-namespace-005/cpu.wsp",
		"/service-00/server-002/metric-namespace-004/6f31f9305c67895c.wsp",
		"/service-00/server-002/metric-namespace-004/cpu.wsp",

		"/service-01/server-110/metric-namespace-007/cpu.wsp",
		"/service-01/server-120/metric-namespace-007/cpu.wsp",
		"/service-01/server-170/metric-namespace-007/cpu.wsp",

		"/service-01/server-114/metric-namespace-007/cpu.wsp",
		"/service-01/server-125/metric-namespace-007/cpu.wsp",
		"/service-01/server-12a/metric-namespace-007/cpu.wsp",
		"/service-01/server-149/metric-namespace-007/cpu.wsp",

		"/service-01/server-125/metric-namespzce-007/cpu.wsp",

		"/service-01/server-170/metric-namespace-004-007-xdp/cpu.wsp",
		"/service-01/server-170/metric-namespace-007-007-xdp/cpu.wsp",
		"/service-01/server-170/metric-namespace-007-005-xdp/cpu.wsp",
		"/service-01/server-170/metric-namespace-007-008-xdp/cpu.wsp",
		"/service-01/server-170/metric-namespace-006-xdp/cpu.wsp",
	}
	var cases = []struct {
		input       []string
		query       string
		hasError    bool
		expect      []string
		expectLeafs []bool
	}{
		{
			input:  commonFiles,
			query:  "service-00",
			expect: []string{"service-00"},
		},
		{
			input:  commonFiles,
			query:  "service-00.server-000.metric-namespace-000.cpu",
			expect: []string{"service-00.server-000.metric-namespace-000.cpu"},
		},

		{
			input: commonFiles,
			query: "service-00.server-000.metric-namespace-00[0-2].cpu",
			expect: []string{
				"service-00.server-000.metric-namespace-000.cpu",
				"service-00.server-000.metric-namespace-001.cpu",
				"service-00.server-000.metric-namespace-002.cpu",
			},
		},
		{
			input: commonFiles,
			query: "service-00.server-00[0-2].metric-namespace-00[0-2].cpu",
			expect: []string{
				"service-00.server-000.metric-namespace-000.cpu",
				"service-00.server-000.metric-namespace-001.cpu",
				"service-00.server-000.metric-namespace-002.cpu",
				"service-00.server-001.metric-namespace-002.cpu",
			},
		},
		{
			input: commonFiles,
			query: "service-01.server-1[0-2]0.metric-namespace-007.cpu",
			expect: []string{
				"service-01.server-110.metric-namespace-007.cpu",
				"service-01.server-120.metric-namespace-007.cpu",
			},
		},
		{
			input: commonFiles,
			query: "service-01.server-1[0-5][4-5a-z].metric-namespace-007.cpu",
			expect: []string{
				"service-01.server-114.metric-namespace-007.cpu",
				"service-01.server-125.metric-namespace-007.cpu",
				"service-01.server-12a.metric-namespace-007.cpu",
			},
		},
		{
			input: commonFiles,
			query: "service-01.server-1[1]4.metric-namespace-007.cpu",
			expect: []string{
				"service-01.server-114.metric-namespace-007.cpu",
			},
		},
		{
			input: commonFiles,
			query: "service-01.server-1[0-2][4-5].metric-namespace-007.cpu",
			expect: []string{
				"service-01.server-114.metric-namespace-007.cpu",
				"service-01.server-125.metric-namespace-007.cpu",
			},
		},
		{
			input: []string{
				"/service-01/server-114/metric-namespace-007/cpu.wsp",
				"/service-01/server-125/metric-namespace-007/cpu.wsp",
				"/service-01/server-125/metric-namespace-006/cpu.wsp",
				"/service-01/server-111/metric-namespace-007/cpu.wsp",
				"/service-01/server-11a/metric-namespace-007/cpu.wsp",
				"/service-01/something-125/metric-namespace-007/cpu.wsp",
			},
			query: "service-01.server-1[0-2][^4-5].metric-namespace-007.cpu",
			expect: []string{
				"service-01.server-111.metric-namespace-007.cpu",
				"service-01.server-11a.metric-namespace-007.cpu",
			},
		},
		{
			input: commonFiles,
			query: "service-01.server-1[0-2][4-5].metric-n[a-z]mesp[a-z1-9]ce-007.cpu",
			expect: []string{
				"service-01.server-114.metric-namespace-007.cpu",
				"service-01.server-125.metric-namespace-007.cpu",
				"service-01.server-125.metric-namespzce-007.cpu",
			},
		},

		{
			input: commonFiles,
			query: "service-00.*.metric-namespace-005.cpu",
			expect: []string{
				"service-00.server-000.metric-namespace-005.cpu",
				"service-00.server-001.metric-namespace-005.cpu",
				"service-00.server-002.metric-namespace-005.cpu",
			},
		},

		{
			input: commonFiles,
			query: "*",
			expect: []string{
				"service-00",
				"service-01",
				"something",
			},
		},
		{
			input: commonFiles,
			query: "",
			expect: []string{
				"service-00",
				"service-01",
				"something",
			},
		},
		{
			input: commonFiles,
			query: "\t",
			expect: []string{
				"service-00",
				"service-01",
				"something",
			},
		},
		{
			input: commonFiles,
			query: " ",
			expect: []string{
				"service-00",
				"service-01",
				"something",
			},
		},
		{
			input: append(commonFiles,
				"/ooo/abc.wsp",
				"/ooo/abc/xxx.wsp",
				"/ooo/abd/xxx.wsp",
				"/ooo/ab/xxx.wsp",
				"/ooo/abcd/xxx.wsp",
				"/ooo/xyz/xxx.wsp",
				"/ooo/xy/xxx.wsp",
			),
			query: "*.*",
			expect: []string{
				"ooo.ab",
				"ooo.abc",
				"ooo.abc",
				"ooo.abcd",
				"ooo.abd",
				"ooo.xy",
				"ooo.xyz",
				"service-00.server-000",
				"service-00.server-001",
				"service-00.server-002",
				"service-01.server-000",
				"service-01.server-110",
				"service-01.server-114",
				"service-01.server-120",
				"service-01.server-125",
				"service-01.server-12a",
				"service-01.server-149",
				"service-01.server-170",
				"something.else",
			},
		},
		{
			// case 1:
			// 	abc . xxx
			// 	ab  . xxx
			// case 2:
			// 	ab  . xxx
			// 	abc . xxx
			// case 3:
			// 	abc . xxx
			// 	abc . xxx
			// case 4:
			// 	abc . xxx
			// 	xyz . xxx
			// case 5:
			// 	abc . xxx
			// 	acc . xxx
			// case 6:
			//  abc . xxx
			//  abd . xxx
			// case 7:
			//  abc  . xxx
			//  abde . xxx
			input: append(commonFiles,
				"/ooo/abc/xxx.wsp",
				"/ooo/ab/xxx.wsp",
			),
			query: "*.*.*",
			expect: []string{
				"ooo.ab.xxx",
				"ooo.abc.xxx",
				"service-00.server-000.metric-namespace-000",
				"service-00.server-000.metric-namespace-001",
				"service-00.server-000.metric-namespace-002",
				"service-00.server-000.metric-namespace-005",
				"service-00.server-001.metric-namespace-002",
				"service-00.server-001.metric-namespace-005",
				"service-00.server-002.metric-namespace-003",
				"service-00.server-002.metric-namespace-004",
				"service-00.server-002.metric-namespace-005",
				"service-01.server-000.metric-namespace-004",
				"service-01.server-000.metric-namespace-005",
				"service-01.server-110.metric-namespace-007",
				"service-01.server-114.metric-namespace-007",
				"service-01.server-120.metric-namespace-007",
				"service-01.server-125.metric-namespace-007",
				"service-01.server-125.metric-namespzce-007",
				"service-01.server-12a.metric-namespace-007",
				"service-01.server-149.metric-namespace-007",
				"service-01.server-170.metric-namespace-004-007-xdp",
				"service-01.server-170.metric-namespace-006-xdp",
				"service-01.server-170.metric-namespace-007",
				"service-01.server-170.metric-namespace-007-005-xdp",
				"service-01.server-170.metric-namespace-007-007-xdp",
				"service-01.server-170.metric-namespace-007-008-xdp",
				"something.else.server",
			},
		},
		{
			input: commonFiles,
			query: "service-0*.*.metric-namespace-005.cpu",
			expect: []string{
				"service-00.server-000.metric-namespace-005.cpu",
				"service-00.server-001.metric-namespace-005.cpu",
				"service-00.server-002.metric-namespace-005.cpu",
				"service-01.server-000.metric-namespace-005.cpu",
			},
		},
		{
			input: commonFiles,
			query: "service-0*.*.metric-*-00[5-7].cpu",
			expect: []string{
				"service-00.server-000.metric-namespace-005.cpu",
				"service-00.server-001.metric-namespace-005.cpu",
				"service-00.server-002.metric-namespace-005.cpu",
				"service-01.server-000.metric-namespace-005.cpu",
				"service-01.server-110.metric-namespace-007.cpu",
				"service-01.server-114.metric-namespace-007.cpu",
				"service-01.server-120.metric-namespace-007.cpu",
				"service-01.server-125.metric-namespace-007.cpu",
				"service-01.server-125.metric-namespzce-007.cpu",
				"service-01.server-12a.metric-namespace-007.cpu",
				"service-01.server-149.metric-namespace-007.cpu",
				"service-01.server-170.metric-namespace-007.cpu",
			},
		},
		{
			input: commonFiles,
			query: "service-0*.*.metric-*-00[5-7]-xdp.cpu",
			expect: []string{
				"service-01.server-170.metric-namespace-004-007-xdp.cpu",
				"service-01.server-170.metric-namespace-006-xdp.cpu",
				"service-01.server-170.metric-namespace-007-005-xdp.cpu",
				"service-01.server-170.metric-namespace-007-007-xdp.cpu",
			},
		},
		{
			input: commonFiles,
			query: "service-0*.*.{metric-namespace-004-007-xdp,metric-namespace-007-007-xdp}.cpu",
			expect: []string{
				"service-01.server-170.metric-namespace-004-007-xdp.cpu",
				"service-01.server-170.metric-namespace-007-007-xdp.cpu",
			},
		},
		{
			input: commonFiles,
			query: "service-0*.*.metric-namespace-{{004,007}}{-007}-xdp.cpu",
			expect: []string{
				"service-01.server-170.metric-namespace-004-007-xdp.cpu",
				"service-01.server-170.metric-namespace-007-007-xdp.cpu",
			},
		},
		{
			// reason: should no panic over bad queries
			input:    commonFiles,
			query:    "service-01.server-170.metric-namespace-004-007-xdp.cp[u",
			hasError: true,
		},
		{
			input: []string{
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/http_3xx.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/http_5xx.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/http_other.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/http_4xx.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/tcp.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/udp.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/backend/random-404_xoxo/http_xxx.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/os/random-404_xoxo/http_xxx.wsp",
			},
			query: "services.groups.*.*.nginx.type.*.{{frontend,backend},os}.random-404_xoxo.http*",
			expect: []string{
				"services.groups.xyz.xxx_404.nginx.type.prod.backend.random-404_xoxo.http_xxx",
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random-404_xoxo.http_3xx",
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random-404_xoxo.http_4xx",
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random-404_xoxo.http_5xx",
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random-404_xoxo.http_other",
				"services.groups.xyz.xxx_404.nginx.type.prod.os.random-404_xoxo.http_xxx",
			},
		},
		{
			input: []string{
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/http_3xx.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/http_5xx.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/http_other.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/http_4xx.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/tcp.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/udp.wsp",
			},
			query: "services.groups.*.*.nginx.type.*.frontend.random-404_xoxo.http*",
			expect: []string{
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random-404_xoxo.http_3xx",
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random-404_xoxo.http_4xx",
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random-404_xoxo.http_5xx",
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random-404_xoxo.http_other",
			},
		},
		{
			input: []string{
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/http_3xx.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/http_5xx.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/http_other.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/http_4xx.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/tcp.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/udp.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random/404/xoxo/http_other.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random/404/xoxo/udp.wsp",
			},
			query: "services.groups.*.*.nginx.type.*.frontend.{random-404_xoxo,random.404.xoxo}.http*",
			expect: []string{
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random-404_xoxo.http_3xx",
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random-404_xoxo.http_4xx",
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random-404_xoxo.http_5xx",
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random-404_xoxo.http_other",
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random.404.xoxo.http_other",
			},
		},
		{
			input: []string{
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/http_3xx.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/http_5xx.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/http_other.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/http_4xx.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-403_xoxo/http_4xx.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/tcp.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/udp.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random/404/xoxo/http_other.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random/401/xoxo/http_other.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random/404/xoxo/udp.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random/403/xoxo/udp.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random/4044/xoxo/http.wsp",
			},
			query: "services.groups.*.*.nginx.type.*.frontend.{random-40?_xoxo,random.40?.xoxo}.http*",
			expect: []string{
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random-403_xoxo.http_4xx",
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random-404_xoxo.http_3xx",
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random-404_xoxo.http_4xx",
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random-404_xoxo.http_5xx",
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random-404_xoxo.http_other",
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random.401.xoxo.http_other",
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random.404.xoxo.http_other",
			},
		},
		{
			input: []string{
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/http_3xx.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/http_5xx.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/http_other.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/http_4xx.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-403_xoxo/http_4xx.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/tcp.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/udp.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random/404/xoxo/http_other.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random/401/xoxo/http_other.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random/404/xoxo/udp.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random/403/xoxo/udp.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random/4044/xoxo/http.wsp",
			},
			query: "services.groups.*.*.nginx.type.*.frontend.{random-40?_xoxo,random.40?.xoxo}.http_[^5]*",
			expect: []string{
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random-403_xoxo.http_4xx",
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random-404_xoxo.http_3xx",
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random-404_xoxo.http_4xx",
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random-404_xoxo.http_other",
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random.401.xoxo.http_other",
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random.404.xoxo.http_other",
			},
		},
		{
			input: []string{
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/http_3xx.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-403_xoxo/http_5xx.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/http_other.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/http_4xx.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-403_xoxo/http_4xx.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/tcp.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/udp.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random/404/xoxo/http_other.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random/401/xoxo/http_other.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random/404/xoxo/udp.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random/403/xoxo/udp.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random/4044/xoxo/http.wsp",
			},
			query: "services.groups.*.*.nginx.type.*.frontend.*404_xoxo.http*",
			expect: []string{
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random-404_xoxo.http_3xx",
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random-404_xoxo.http_4xx",
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random-404_xoxo.http_other",
			},
		},
		{
			input: []string{
				"/fe/series/abc_101/xyz/haproxy/host/cjk-1018_main7_internet_com/traffic.wsp",
				"/fe/series/abc_101/xyz/haproxy/host/cjk-1019_main7_internet_com/traffic.wsp",
				"/fe/series/abc_101/xyz/haproxy/host/cjk-1020_main7_internet_com/traffic.wsp",
				"/fe/series/abc_101/xyz/haproxy/host/cjk-2022_expr1_internet_com/traffic.wsp",
				"/fe/series/abc_101/xyz/haproxy/host/mno-2022_expr1_internet_com/traffic.wsp",
			},
			query: "fe.series.*.*.haproxy.host.*cjk-*_internet_com.traffic",
			expect: []string{
				"fe.series.abc_101.xyz.haproxy.host.cjk-1018_main7_internet_com.traffic",
				"fe.series.abc_101.xyz.haproxy.host.cjk-1019_main7_internet_com.traffic",
				"fe.series.abc_101.xyz.haproxy.host.cjk-1020_main7_internet_com.traffic",
				"fe.series.abc_101.xyz.haproxy.host.cjk-2022_expr1_internet_com.traffic",
			},
		},
		{
			input: []string{
				"/fe/series/abc_101/xyz/haproxy/host/cjk-1018_main7_internet_com/traffic.wsp",
				"/fe/series/abc_101/xyz/haproxy/host/cjk-1019_main7_internet_com/traffic.wsp",
				"/fe/series/abc_101/xyz/haproxy/host/cjk-1020_main7_internet_com/traffic.wsp",
				"/fe/series/abc_101/xyz/haproxy/host/cjk-2022_expr1_internet_com/traffic.wsp",
				"/fe/series/abc_101/xyz/haproxy/host/mno-2022_expr1_internet_com/traffic.wsp",
			},
			query: "fe.series.*.*...haproxy.host.*cjk-*_internet_com.traffic",
			expect: []string{
				"fe.series.abc_101.xyz.haproxy.host.cjk-1018_main7_internet_com.traffic",
				"fe.series.abc_101.xyz.haproxy.host.cjk-1019_main7_internet_com.traffic",
				"fe.series.abc_101.xyz.haproxy.host.cjk-1020_main7_internet_com.traffic",
				"fe.series.abc_101.xyz.haproxy.host.cjk-2022_expr1_internet_com.traffic",
			},
		},
		{
			input: []string{
				"/fe/series/abc_101/xyz/haproxy/host/cjk-1018_main7_internet_com/traffic.wsp",
				"/fe/series/abc_101/xyz/haproxy/host/cjk-1019_main7_internet_com/traffic.wsp",
				"/fe/series/abc_101/xyz/haproxy/host/cjk-1020_main7_internet_com/traffic.wsp",
				"/fe/series/abc_101/xyz/haproxy/host/cjk-2022_expr1_internet_com/traffic.wsp",
				"/fe/series/abc_101/xyz/haproxy/host/mno-2022_expr1_internet_com/traffic.wsp",
			},
			query: "fe.series.*.*.haproxy.host.*cjk-*_internet_com.traffic.",
			expect: []string{
				"fe.series.abc_101.xyz.haproxy.host.cjk-1018_main7_internet_com.traffic",
				"fe.series.abc_101.xyz.haproxy.host.cjk-1019_main7_internet_com.traffic",
				"fe.series.abc_101.xyz.haproxy.host.cjk-1020_main7_internet_com.traffic",
				"fe.series.abc_101.xyz.haproxy.host.cjk-2022_expr1_internet_com.traffic",
			},
		},
		{
			input: []string{
				"/fe/series/abc_101/xyz/haproxy/host/cjk-1018_main7_internet_com/traffic.wsp",
				"/fe/series/abc_101/xyz/haproxy/host/cjk-1019_main7_internet_com/traffic.wsp",
				"/fe/series/abc_101/xyz/haproxy/host/cjk-1020_main7_internet_com/traffic.wsp",
				"/fe/series/abc_101/xyz/haproxy/host/cjk-2022_expr1_internet_com/traffic.wsp",
				"/fe/series/abc_101/xyz/haproxy/host/mno-2022_expr1_internet_com/traffic.wsp",
			},
			query: "...fe.series.*.*.haproxy.host.*cjk-*_internet_com.traffic",
			expect: []string{
				"fe.series.abc_101.xyz.haproxy.host.cjk-1018_main7_internet_com.traffic",
				"fe.series.abc_101.xyz.haproxy.host.cjk-1019_main7_internet_com.traffic",
				"fe.series.abc_101.xyz.haproxy.host.cjk-1020_main7_internet_com.traffic",
				"fe.series.abc_101.xyz.haproxy.host.cjk-2022_expr1_internet_com.traffic",
			},
		},
		{
			input: []string{
				// NOTE: ordering here is important, for trieIndex.insert case 8
				"/ns1/ns2/ns3/ns4/ns5/ns6/ns7_handle/val.wsp",
				"/ns1/ns2/ns3/ns4/ns5/ns6/ns7_handle.wsp",
			},
			query: "ns1.ns2.ns3.ns4.ns5.ns6.*",
			expect: []string{
				"ns1.ns2.ns3.ns4.ns5.ns6.ns7_handle",
				"ns1.ns2.ns3.ns4.ns5.ns6.ns7_handle",
			},
			expectLeafs: []bool{false, true},
		},
		{
			input: []string{
				"/ns1/ns2/ns3/ns4/ns5/ns6/ns7_handle.wsp",
				"/ns1/ns2/ns3/ns4/ns5/ns6/ns7.wsp",
			},
			query: "ns1.ns2.ns3.ns4.ns5.ns6.*",
			expect: []string{
				"ns1.ns2.ns3.ns4.ns5.ns6.ns7_handle",
				"ns1.ns2.ns3.ns4.ns5.ns6.ns7",
			},
			expectLeafs: []bool{true, true},
		},
		{
			input: []string{
				"/系统/核心/cpu.wsp",
				"/系统/核心/memory.wsp",
				"/ns1/ns2/ns3/ns4/ns5/ns6/ns7_handle.wsp",
				"/ns1/ns2/ns3/ns4/ns5/ns6/ns7.wsp",
			},
			query: "系统.核心.*",
			expect: []string{
				"系统.核心.cpu",
				"系统.核心.memory",
			},
			expectLeafs: []bool{true, true},
		},
		{
			input: []string{
				"/ns1/ns2/ns3/ns4/ns5/ns6/ns7_handle.wsp",
				"/ns1/ns2/ns3/ns4/ns5/ns6/.wsp", // should not panic
			},
			query: "*",
			expect: []string{
				"ns1",
			},
			expectLeafs: []bool{false},
		},
		{
			input: []string{
				"/ns1/ns2/ns3/ns4/ns5/ns6/ns7_handle.wsp",
				"./..wsp",
				"..wsp", // should not panic
			},
			query: "*",
			expect: []string{
				".", // should we even support . as filename?
				"ns1",
			},
			expectLeafs: []bool{true, false},
		},
		{
			input: []string{
				"/ns1/ns2/ns3/ns4/ns5/ns6/ns7_handle.wsp",
				"/ns1/ns2/ns3/ns4/ns5/ns6_1/",
				"/ns1/ns2/ns3/ns4/ns5/ns6_2",
				"/ns1/ns2/ns3/ns4/ns5/ns6_3/",
				"/ns1/ns2/ns3/ns4/ns5/ns6_3/",
				"/ns1/ns2/ns3/ns4/ns5/ns6_3/",
				"/ns1/ns2/ns3/ns4/ns5/ns6_3/",
				"/ns1/ns2/ns3/ns4/ns5/ns6_3/metric.wsp",
			},
			query: "ns1.ns2.ns3.ns4.ns5.*",
			expect: []string{
				"ns1.ns2.ns3.ns4.ns5.ns6",
				"ns1.ns2.ns3.ns4.ns5.ns6_1",
				"ns1.ns2.ns3.ns4.ns5.ns6_2",
				"ns1.ns2.ns3.ns4.ns5.ns6_3",
			},
			expectLeafs: []bool{false, false, false, false},
		},
		{
			input: []string{
				"zk/kafka_xxx/by_node/node_0/status/health.wsp",
				"zk/streaming_yyy/by_node/node_0/status/health.wsp",
				"zk/kafka_zzz/by_node/node_0/status", // intentionally empty directory node
			},
			query: "zk.*.by_node.*.status.health",
			expect: []string{
				"zk.kafka_xxx.by_node.node_0.status.health",
				"zk.streaming_yyy.by_node.node_0.status.health",
			},
			expectLeafs: []bool{true, true},
		},
	}

	for _, c := range cases {
		t.Run(c.query, func(t *testing.T) {
			t.Logf("case: TestTrieIndex/'^%s$'", regexp.QuoteMeta(c.query))

			trieServer := newTrieServer(c.input, false, t)
			resultCh := make(chan *ExpandedGlobResponse, 1)
			trieServer.expandGlobs(context.TODO(), c.query, resultCh)
			result := <-resultCh
			trieFiles, err := result.Files, result.Err

			// trieFiles, _, err := trieServer.expandGlobsTrie(c.query)
			if err != nil && !c.hasError {
				t.Errorf("failed to trie.expandGlobs: %s", err)
			}

			if c.expectLeafs != nil {
				for i := range c.expect {
					c.expect[i] += fmt.Sprintf(" %t", c.expectLeafs[i])
				}
				for i := range trieFiles {
					trieFiles[i] += fmt.Sprintf(" %t", result.Leafs[i])
				}
			}

			// trieServer.CurrentFileIndex().trieIdx.dump(os.Stdout)

			sort.Strings(trieFiles)
			sort.Strings(c.expect)
			if !reflect.DeepEqual(trieFiles, c.expect) {
				t.Errorf("incorrect files retrieved\nreturns: %s\nexpect:  %s\n", trieFiles, c.expect)
			}
		})
	}
}

func TestTrieEdgeCases(t *testing.T) {
	var trie = newTrie(".wsp", nil)

	_, _, _, _, err := trie.query("[\xff\xff-\xff", 1000, func([]string) ([]string, error) { return nil, nil })
	if err == nil || err.Error() != "glob: range overflow" {
		t.Errorf("trie should return an range overflow error")
	}

	if err := trie.insert("ns1/ns2/ns3/ns4/ns5/ns7/", 0, 0, 0); err != nil {
		t.Errorf("should not return insert error when inserting folders")
	}
}

func TestTrieQueryOpts(t *testing.T) {
	var trieIndex = newTrie(".wsp", nil)

	trieIndex.insert("/sys/app/host-01.wsp", 0, 0, 0)
	trieIndex.insert("/sys/app/host-01/cpu/user.wsp", 0, 0, 0)
	trieIndex.insert("/sys/app/host-01/cpu/system.wsp", 0, 0, 0)
	trieIndex.insert("/sys/app/host-01/memory/cache.wsp", 0, 0, 0)
	trieIndex.insert("/sys/app/host-02/cpu/user.wsp", 0, 0, 0)
	trieIndex.insert("/sys/app/host-02/cpu/system.wsp", 0, 0, 0)
	trieIndex.insert("/sys/app/host-03/cpu/system.wsp", 0, 0, 0)

	wantMetrics := []string{
		"sys.app.host-01",
		"sys.app.host-01.cpu.user",
		"sys.app.host-01.cpu.system",
		"sys.app.host-01.memory.cache",
		"sys.app.host-02.cpu.user",
		"sys.app.host-02.cpu.system",
	}

	files, _, nodes, _, err := trieIndex.query("sys/app/host-0{1,2}", 1000, nil)
	if err != nil {
		t.Error(err)
	}

	var metrics []string
	for i, n := range nodes {
		if n.file() {
			metrics = append(metrics, files[i])
			continue
		}

		nmetrics, _, _, _, _ := trieIndex.allMetricsNode(n, '.', files[i], 65536, false)
		metrics = append(metrics, nmetrics...)
	}

	if got, want := metrics, wantMetrics; !reflect.DeepEqual(got, want) {
		t.Errorf("metrics = %s; want %s", got, want)
	}
}

func TestTrieConcurrentReadWrite(t *testing.T) {
	trieIndex := newTrie(".wsp", nil)

	rand.Seed(time.Now().Unix())

	var donec = make(chan bool)
	// var filec = make(chan string)
	var filem sync.Map
	var factor = 100
	go func() {
		for run := 0; run < 3; run++ {
			for i := 0; i < factor; i++ {
				for j := 0; j < factor; j++ {
					for k := 0; k < factor; k++ {
						filem.Store(fmt.Sprintf("level-0-%d.level-1-%d.level-2-%d", i, j, k), true)
						trieIndex.insert(fmt.Sprintf("/level-0-%d/level-1-%d/level-2-%d.wsp", i, j, k), 0, 0, 0)
						// if (i+j+k)%5 == 0 {
						// 	time.Sleep(time.Millisecond * time.Duration(rand.Intn(10)))
						// }
					}
				}
			}

			trieIndex.prune()
			trieIndex.root.gen++
		}

		donec <- true
	}()

	// time.Sleep(time.Second)

	var zero int
	for {
		select {
		case <-donec:
			t.Logf("total zero result: %d/%d", zero, factor*factor*factor)
			return
		// case <-filec:
		default:
			// skipcq: GSC-G404
			files, _, _, _, err := trieIndex.query(fmt.Sprintf("level-0-%d/level-1-%d/level-2-%d*", rand.Intn(factor), rand.Intn(factor), rand.Intn(factor)), int(math.MaxInt64), nil)
			if err != nil {
				panic(err)
			}
			if len(files) > 0 {
				for _, f := range files {
					if _, ok := filem.Load(f); !ok {
						t.Errorf("trie index returned an unknown file: %s", f)
					}
				}
			} else {
				zero++
			}
		}
	}
}

// for fixing Unused code error from deepsource.io, dump is useful for debugging
var _ = (&trieIndex{}).dump

func TestTriePrune(t *testing.T) {
	cases := []struct {
		files1 []string
		files2 []string
		expect []string
	}{
		0: {
			files1: []string{
				"/level-0/level-1/level-2/memory.wsp",
				"/level-0/level-1/level-2-1/memory.wsp",
				"/level-0/level-1/level-2-2.wsp",
				"/level-0/level-1/level-2-2/memory1.wsp",
				"/level-0/level-1/level-2-2/memory.wsp",
				"/level-0/level-1/level-2-2/memory2.wsp",
				"/level-0/level-1/cpu.wsp",
				"/level-0/disk.wsp",
			},
			files2: []string{
				"/level-0/level-1/cpu.wsp",
				"/level-0/level-1/level-2-2/memory.wsp",
				"/level-0/disk.wsp",
			},
			expect: []string{
				"level-0.disk",
				"level-0.level-1.cpu",
				"level-0.level-1.level-2-2.memory",
			},
		},
		1: {
			files1: []string{
				"/abc.wsp",
				"/abd.wsp",
				"/adc.wsp",
			},
			files2: []string{
				"/abc.wsp",
				"/xyz.wsp",
			},
			expect: []string{
				"abc",
				"xyz",
			},
		},
		2: {
			files1: []string{
				"prefix-1006_xxx/rate.wsp",
			},
			files2: []string{
				"prefix-1010_xxx/chkdown.wsp",
				"prefix-1005_xxx/bin.wsp",
				"prefix-1001_xxx/lbtot.wsp",
				"prefix-1003_xxx/hrsp_1xx.wsp",
				"prefix-1007_xxx/smax.wsp",
				"prefix-1008_xxx/bout.wsp",
			},
			expect: []string{
				"prefix-1001_xxx.lbtot",
				"prefix-1003_xxx.hrsp_1xx",
				"prefix-1005_xxx.bin",
				"prefix-1007_xxx.smax",
				"prefix-1008_xxx.bout",
				"prefix-1010_xxx.chkdown",
			},
		},
	}

	for i, c := range cases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			ctrieIndex := newTrie(".wsp", nil)
			strieIndex := newTrie(".wsp", nil)

			for _, f := range c.files1 {
				ctrieIndex.insert(f, 0, 0, 0)
			}

			ctrieIndex.root.gen++
			for _, f := range c.files2 {
				ctrieIndex.insert(f, 0, 0, 0)
				strieIndex.insert(f, 0, 0, 0)
			}

			ctrieIndex.prune()

			if got, want := ctrieIndex.allMetrics('.'), c.expect; !reflect.DeepEqual(got, want) {
				t.Errorf("g = %s; want %s", got, want)
			}

			ccount, cfiles, cdirs, conec, conefc, conedc, ccountByChildren, _ := ctrieIndex.countNodes()
			scount, sfiles, sdirs, sonec, sonefc, sonedc, scountByChildren, _ := strieIndex.countNodes()
			if ccount != scount {
				t.Errorf("ccount = %v; scount %v", ccount, scount)
			}
			if cfiles != sfiles {
				t.Errorf("cfiles = %v; sfiles %v", cfiles, sfiles)
			}
			if cdirs != sdirs {
				t.Errorf("cdirs = %v; sdirs %v", cdirs, sdirs)
			}
			if conec != sonec {
				t.Errorf("conec = %v; sonec %v", conec, sonec)
			}
			if conefc != sonefc {
				t.Errorf("conefc = %v; sonefc %v", conefc, sonefc)
			}
			if conedc != sonedc {
				t.Errorf("conedc = %v; sonedc %v", conedc, sonedc)
			}
			if *ccountByChildren != *scountByChildren {
				t.Errorf("ccountByChildren = %s; scountByChildren %s", ccountByChildren, scountByChildren)
			}
		})
	}
}

func BenchmarkGlobIndex(b *testing.B) {
	var btrieServer *CarbonserverListener
	var btrigramServer *CarbonserverListener

	// 10/100/50000
	limit0 := 1      // 50
	limit1 := 150000 // 50
	limit2 := 1      // 100
	limit3 := 1      // 1
	files := make([]string, 0, limit0*limit1*limit2*limit3)
	rand.Seed(time.Now().Unix())
	for i := 0; i < limit0; i++ {
		for j := 0; j < limit1; j++ {
			for k := 0; k < limit2; k++ {
				// for l := 0; l < limit3; l++ {
				// 	files = append(files, fmt.Sprintf("/service-%02d/server-%03d/metric-namespace-%03d/%x.wsp", i, j, k, rand.Int()))
				// }
				files = append(files, fmt.Sprintf("/service-%02d/server-%03d/metric-namespace-%03d/cpu.wsp", i, j, k))
			}
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		btrieServer = newTrieServer(files, true, b)
		wg.Done()
	}()
	go func() {
		btrigramServer = newTrigramServer(files, b)
		wg.Done()
	}()

	wg.Wait()

	// b.Logf("memory.Sizeof(btrieServer)    = %+v\n", memory.Sizeof(btrieServer))
	// b.Logf("memory.Sizeof(btrigramServer) = %+v\n", memory.Sizeof(btrigramServer))

	var cases = []struct {
		input string
	}{
		{"service-*.server-*.metric-namespace-[4]0*.*"},
		{"service-*.server-*.*.cpu"},
		{"service-2[3-4].server-02[1-9].metric-namespace-0[2-3]0.cpu"},

		// for exact match, trigram index falls back to file system stat, so
		// this case isn't very representaive, however, trie+dfa performs
		// extremely good in this case
		{"service-23.server-029.metric-namespace-030.cpu"},

		{"service-*.server-*.metric-namespace-40*.*"},
		// trigram index performs is better for this case because trie+dfa needs
		// to scan every third nodes because of leading star in the query
		// "*-40*"
		{"service-*.server-*.*-40*.*"},
		{"service-*.*server-300*.*-40*.*"},
		{"service-*.*server-300*.*-4*.*"},
	}

	for _, c := range cases {
		var ctrie, ctrigram int
		b.Run("trie/"+c.input, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				resultCh := make(chan *ExpandedGlobResponse, 1)
				btrieServer.expandGlobs(context.TODO(), c.input, resultCh)
				ctrie = len((<-resultCh).Files)
			}
		})
		b.Run("trigram/"+c.input, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				resultCh := make(chan *ExpandedGlobResponse, 1)
				btrigramServer.expandGlobs(context.TODO(), c.input, resultCh)
				ctrigram = len((<-resultCh).Files)
			}
		})
		b.Logf("trie.len = %d trigram.len = %d", ctrie, ctrigram)
	}
}

func TestDumpAllMetrics(t *testing.T) {
	files := []string{
		"/bobo.wsp",
		"/bobododo/xxx.wsp",

		"/something.wsp",
		"/somet/xxx.wsp",
		"/something/else/server.wsp",
		"/service-00/server-000/metric-namespace-000/43081e003a315b88.wsp",
		"/service-00/server-000/metric-namespace-000/cpu.wsp",
		"/service-00/server-000/metric-namespace-001/d218bc1539f2cf8.wsp",
		"/service-00/server-000/metric-namespace-001/cpu.wsp",
		"/service-00/server-000/metric-namespace-005/cpu.wsp",
		"/service-00/server-000/metric-namespace-002/29370bc791c0fccb.wsp",
		"/service-00/server-000/metric-namespace-002/cpu.wsp",
		"/service-01/server-000/metric-namespace-002/cpu.wsp",
		"/service-01/switch-000/metric-namespace-002/cpu.wsp",

		"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/http_3xx.wsp",
		"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/http_5xx.wsp",
		"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/http_other.wsp",
		"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/http_4xx.wsp",
		"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/tcp.wsp",
		"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/udp.wsp",
		"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random/404/xoxo/http_other.wsp",
		"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random/404/xoxo/udp.wsp",
		"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_koko_abc/xoxo/udp.wsp",

		"/service-01/server-114/metric-namespace-007/cpu.wsp",
		"/service-01/server-125/metric-namespace-007/cpu.wsp",
		"/service-01/server-12a/metric-namespace-007/cpu.wsp",
		"/service-01/server-149/metric-namespace-007/cpu.wsp",
		"/service-01/server-125/metric-namespzce-007/cpu.wsp",
		"/service-01/server-170/metric-namespace-004-007-xdp/cpu.wsp",
		"/service-01/server-170/metric-namespace-007-007-xdp/cpu.wsp",
		"/service-01/server-170/metric-namespace-007-005-xdp/cpu.wsp",
		"/service-01/server-170/metric-namespace-007-008-xdp/cpu.wsp",
		"/service-01/server-170/metric-namespace-006-xdp/cpu.wsp",
	}
	trie := newTrieServer(files, true, t)
	metrics := trie.CurrentFileIndex().trieIdx.allMetrics('.')
	for i := range files {
		files[i] = files[i][:len(files[i])-4]
		files[i] = files[i][1:]
		files[i] = strings.ReplaceAll(files[i], "/", ".")
	}
	sort.Strings(files)
	sort.Strings(metrics)
	if !reflect.DeepEqual(files, metrics) {
		t.Errorf("trie.allMetrics:\nwant: %s\ngot:  %s\n", files, metrics)
	}
}

func TestTrieQuotaGeneral(t *testing.T) {
	type metric struct {
		name         string
		logicalSize  int64
		physicalSize int64
		dataPoints   int64
	}
	type throttleTest struct {
		metric   string
		data     []points.Point
		throttle bool
		reason   string
	}

	var cases = []struct {
		input1      []metric
		input2      []metric
		quotas      []*Quota
		tests       []throttleTest
		statMetrics []points.Points
	}{
		{
			input1: (func() (r []metric) {
				for i := 0; i < 5; i++ {
					r = append(
						r,
						metric{fmt.Sprintf("/sys/app/server-%02d/cpu.wsp", i), 12 * 1024, 12 * 1024, 1024},
						metric{fmt.Sprintf("/sys/app/server-%02d/memory.wsp", i), 12 * 1024, 12 * 1024, 1024},
						metric{fmt.Sprintf("/sys/app/server-%02d/iostat.wsp", i), 12 * 1024, 12 * 1024, 1024},
					)
				}

				r = append(
					r,
					metric{fmt.Sprintf("/sys/db/server-%02d/iostat.wsp", 0), 12 * 1024, 12 * 1024, 1024},
					metric{fmt.Sprintf("/user/foo/server-%02d/cpu.wsp", 0), 12 * 1024, 12 * 1024, 1024},
					metric{fmt.Sprintf("/play/foo/server-%02d/cpu.wsp", 0), 12 * 1024, 12 * 1024, 1024},
				)
				return r
			})(),

			input2: []metric{
				{"/sys/kv/server-00/iostat.wsp", 12 * 1024, 12 * 1024, 1024},
				{"/sys/kv/server-01/iostat.wsp", 12 * 1024, 12 * 1024, 1024},
			},

			quotas: []*Quota{
				{
					Pattern:    "/",
					Namespaces: 3,
					Metrics:    60,
				},
				{
					Pattern:    "*",
					Namespaces: 3,
					Metrics:    500,
				},
				{
					Pattern:    "sys",
					Namespaces: 3,
					Metrics:    500,
				},
				{
					Pattern: "sys.*",
					Metrics: 600,
				},
				{
					Pattern:    "sys.kv",
					Namespaces: 2,
					Metrics:    600,
				},
				{
					Pattern: "sys.app.*",
					Metrics: 3,
				},
				{
					Pattern: "sys.app.server-01",
					Metrics: 4,
				},
				{
					Pattern:    "sys.app.server-02",
					Metrics:    4,
					DataPoints: 1024 * 3,
				},
			},
			tests: []throttleTest{
				{"sys.app.server-31.cpu", nil, false, ""},
				{"sys.app.server-00.cpu3", nil, true, "throttled by sys.app.* metrics limit"},

				{"sys.app.server-01.cpu3", nil, false, ""},
				{"sys.app.server-02.cpu3", nil, true, "throttled by sys.app.server-01 dataPoints limit"},

				{"sys.kv.server-01.cpu2", nil, false, ""},
				{"sys.kv.server-02.iostat", nil, true, "throttled by sys.kv namespaces limit"},

				{"foo2.kv.server-02.iostat", nil, true, "throttled by / namespaces limit"},
			},
		},
		{
			input1: (func() (r []metric) {
				for i := 0; i < 5; i++ {
					r = append(
						r,
						metric{fmt.Sprintf("/sys/app/server-%02d/cpu.wsp", i), 12 * 1024, 12 * 1024, 1024},
						metric{fmt.Sprintf("/sys/app/server-%02d/memory.wsp", i), 12 * 1024, 12 * 1024, 1024},
						metric{fmt.Sprintf("/sys/app/server-%02d/iostat.wsp", i), 12 * 1024, 12 * 1024, 1024},
					)
				}

				return r
			})(),

			quotas: []*Quota{
				{
					Pattern:      "/",
					PhysicalSize: 1024 * 12 * 15,
				},
				{
					Pattern: "*",
					Metrics: 500,
				},
			},
			tests: []throttleTest{
				{"sys.app.server-00.cpu", nil, false, ""},
				{"sys.app.server-00.cpu3", nil, true, "throttled by / physicalSize limit"},
			},
		},
		{
			input1: (func() (r []metric) {
				// skipcq: CRT-P0001
				r = append(r, metric{"/sys/app/srv1/nodes/host-01/cpu.wsp", 1, 1, 1})
				r = append(r, metric{"/sys/app/srv1/nodes/host-01/mem.wsp", 1, 1, 1})
				r = append(r, metric{"/sys/app/srv2/nodes/host-01/mem.wsp", 1, 1, 1})

				r = append(r, metric{"/sys/app/srv1/nodes/foo-01/cpu-0.wsp", 1, 1, 1})
				r = append(r, metric{"/sys/app/srv1/nodes/foo-01/cpu-1.wsp", 1, 1, 1})
				r = append(r, metric{"/sys/app/srv1/nodes/foo-01/cpu-2.wsp", 1, 1, 1})

				return r
			})(),

			quotas: []*Quota{
				{
					Pattern:      "/",
					PhysicalSize: 1024 * 12 * 15,
				},
				{
					Pattern: "sys.app.*.nodes.*",
					Metrics: 2,
				},
				{
					Pattern: "sys.app.*.nodes.foo-*",
					Metrics: 4,
				},
			},
			tests: []throttleTest{
				{"sys.app.srv1.nodes.host-01.cpu2", nil, true, "throttled by sys.app.*.nodes.* memtrics limit"},
				{"sys.app.srv1.nodes.foo-01.cpu-3", nil, false, ""},
			},

			statMetrics: []points.Points{
				{Metric: "quota.metrics.sys-app-srv1-nodes-host-01", Data: []points.Point{{Value: 2}}},
				{Metric: "usage.metrics.sys-app-srv1-nodes-host-01", Data: []points.Point{{Value: 2}}},
				{Metric: "throttle.sys-app-srv1-nodes-host-01", Data: []points.Point{{Value: 1}}},

				{Metric: "quota.metrics.sys-app-srv1-nodes-foo-01", Data: []points.Point{{Value: 4}}},
				{Metric: "usage.metrics.sys-app-srv1-nodes-foo-01", Data: []points.Point{{Value: 3}}},
				{Metric: "throttle.sys-app-srv1-nodes-foo-01", Data: []points.Point{{Value: 0}}},

				{Metric: "quota.metrics.sys-app-srv2-nodes-host-01", Data: []points.Point{{Value: 2}}},
				{Metric: "usage.metrics.sys-app-srv2-nodes-host-01", Data: []points.Point{{Value: 1}}},
				{Metric: "throttle.sys-app-srv2-nodes-host-01", Data: []points.Point{{Value: 0}}},

				{Metric: "quota.physical_size.root", Data: []points.Point{{Value: 184320}}},
				{Metric: "usage.physical_size.root", Data: []points.Point{{Value: 6}}},
				{Metric: "throttle.root", Data: []points.Point{{Value: 0}}},
			},
		},
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("case_%d", i), func(t *testing.T) {
			tindex := newTrie(
				".wsp",
				func(metric string) (size, dataPoints int64) {
					return 12 * 1024, 1024
				},
			)

			for _, m := range c.input1 {
				tindex.insert(m.name, m.logicalSize, m.physicalSize, m.dataPoints)
			}
			tindex.applyQuotas(time.Minute, c.quotas...)
			tindex.qauMetrics = nil
			tindex.refreshUsage(nil)

			for _, m := range c.input2 {
				tindex.insert(m.name, m.logicalSize, m.physicalSize, m.dataPoints)
			}

			tindex.applyQuotas(time.Minute, c.quotas...)
			tindex.qauMetrics = nil
			tindex.refreshUsage(nil)

			// tindex.dump(os.Stdout)
			// tindex.getQuotaTree(os.Stdout)

			for _, tt := range c.tests {
				t.Logf("Teseting throttle(%q)", tt.metric)
				if throttle := tindex.throttle(&points.Points{Metric: tt.metric, Data: tt.data}, false); throttle != tt.throttle {
					t.Errorf("throttle(%q) = %t, wants %t (explanation:%q)", tt.metric, throttle, tt.throttle, tt.reason)
				}
			}

			tindex.qauMetrics = nil
			tindex.refreshUsage(nil)

			if c.statMetrics != nil && !reflect.DeepEqual(tindex.qauMetrics, c.statMetrics) {
				t.Errorf("qauMetrics:\n%swants:\n%s", stringifyQuotaPoints(tindex.qauMetrics), stringifyQuotaPoints(c.statMetrics))
			}
		})
	}
}

func stringifyQuotaPoints(ps []points.Points) string {
	var str string
	for _, p := range ps {
		str += fmt.Sprintf("%s %v\n", p.Metric, p.Data[0].Value)
	}
	return str
}

func TestTrieQuotaThroughput(t *testing.T) {
	tindex := newTrie(
		".wsp",
		func(metric string) (size, dataPoints int64) {
			return 12 * 1024, 1024
		},
	)

	tindex.insert("/sys/app/server-001/cpu.wsp", 0, 0, 0)
	tindex.insert("/sys/app/server-002/cpu.wsp", 0, 0, 0)

	tindex.applyQuotas(
		time.Minute,
		&Quota{
			Pattern:      "/",
			PhysicalSize: 1024 * 12 * 15,
		},
		&Quota{
			Pattern:    "sys.app.*",
			Throughput: 5,
		},
	)

	tindex.refreshUsage(tindex.throughputs)

	if tindex.throttle(&points.Points{Metric: "sys.app.server-001.cpu", Data: []points.Point{{}, {}, {}, {}}}, false) {
		t.Errorf("should not throttle old metric within throughput quota")
	}
	if !tindex.throttle(&points.Points{Metric: "sys.app.server-001.cpu", Data: []points.Point{{}, {}, {}, {}}}, false) {
		t.Errorf("should throttle old metric exceeding throughput quota")
	}

	if !tindex.throttle(&points.Points{Metric: "sys.app.server-002.cpu2", Data: []points.Point{{}, {}, {}, {}, {}, {}}}, false) {
		t.Errorf("should throttle new metric exceeding throughput quota")
	}

	tindex.refreshUsage(tindex.throughputs)

	// TODO: check throughput stat metrics
	if wants := []points.Points{
		{Metric: "quota.throughput.sys-app-server-001", Data: []points.Point{{Value: 5}}},
		{Metric: "usage.throughput.sys-app-server-001", Data: []points.Point{{Value: 4}}},
		{Metric: "throttle.sys-app-server-001", Data: []points.Point{{Value: 4}}},

		{Metric: "quota.throughput.sys-app-server-002", Data: []points.Point{{Value: 5}}},
		{Metric: "usage.throughput.sys-app-server-002", Data: []points.Point{{Value: 0}}},
		{Metric: "throttle.sys-app-server-002", Data: []points.Point{{Value: 6}}},

		{Metric: "quota.physical_size.root", Data: []points.Point{{Value: 184320}}},
		{Metric: "usage.physical_size.root", Data: []points.Point{{Value: 24576}}},
		{Metric: "throttle.root", Data: []points.Point{{Value: 0}}},
	}; !reflect.DeepEqual(tindex.qauMetrics, wants) {
		t.Errorf("qauMetrics:\n%swants:\n%s", stringifyQuotaPoints(tindex.qauMetrics), stringifyQuotaPoints(wants))
	}
}

func TestTrieQuotaThroughputWithDelayedReset(t *testing.T) {
	tindex := newTrie(
		".wsp",
		func(metric string) (size, dataPoints int64) {
			return 12 * 1024, 1024
		},
	)

	tindex.insert("/sys/app/server-003/cpu.wsp", 0, 0, 0)

	tindex.applyQuotas(
		time.Minute,
		&Quota{
			Pattern:      "/",
			PhysicalSize: 1024 * 12 * 15,
		},
		&Quota{
			Pattern:    "sys.app.*",
			Throughput: 4,
		},
	)

	tindex.root.gen++
	tindex.insert("/sys/app/server-001/cpu.wsp", 0, 0, 0)
	tindex.insert("/sys/app/server-002/cpu.wsp", 0, 0, 0)
	tindex.prune()

	tindex.applyQuotas(
		time.Minute,
		&Quota{
			Pattern:      "/",
			PhysicalSize: 1024 * 12 * 15,
		},
		&Quota{
			Pattern:    "sys.app.*",
			Throughput: 4,
		},
	)

	var namespaces []string
	tindex.throughputs.entries.Range(func(k, v interface{}) bool {
		namespaces = append(namespaces, k.(string))
		return true
	})
	sort.Strings(namespaces)
	if got, want := namespaces, []string{"/", "sys.app.server-001", "sys.app.server-002"}; !reflect.DeepEqual(got, want) {
		t.Errorf("namespaces = %s; want %s", got, want)
	}

	tindex.refreshUsage(tindex.throughputs)

	if tindex.throttle(&points.Points{Metric: "sys.app.server-001.cpu", Data: []points.Point{{}, {}, {}, {}}}, false) {
		t.Errorf("should not throttle old metric within throughput quota")
	}

	// faking failure of throughput usage reset
	tpe := tindex.throughputs.load("sys.app.server-001")
	tpe.resetAtv.Store(time.Now().Add(time.Minute * -2))

	// failed to perform thoughput usage reset timely, should not throttle metrics in this namespace
	if tindex.throttle(&points.Points{Metric: "sys.app.server-001.cpu", Data: []points.Point{{}, {}, {}, {}}}, false) {
		t.Errorf("should not throttle old metric within throughput quota")
	}

	if !tindex.throttle(&points.Points{Metric: "sys.app.server-002.cpu2", Data: []points.Point{{}, {}, {}, {}, {}, {}}}, false) {
		t.Errorf("should throttle new metric exceeding throughput quota")
	}

	tindex.refreshUsage(tindex.throughputs)

	if wants := []points.Points{
		{Metric: "quota.throughput.sys-app-server-001", Data: []points.Point{{Value: 4}}},
		{Metric: "usage.throughput.sys-app-server-001", Data: []points.Point{{Value: 8}}},
		{Metric: "throttle.sys-app-server-001", Data: []points.Point{{Value: 0}}},

		{Metric: "quota.throughput.sys-app-server-002", Data: []points.Point{{Value: 4}}},
		{Metric: "usage.throughput.sys-app-server-002", Data: []points.Point{{Value: 0}}},
		{Metric: "throttle.sys-app-server-002", Data: []points.Point{{Value: 6}}},

		{Metric: "quota.physical_size.root", Data: []points.Point{{Value: 184320}}},
		{Metric: "usage.physical_size.root", Data: []points.Point{{Value: 24576}}},
		{Metric: "throttle.root", Data: []points.Point{{Value: 0}}},
	}; !reflect.DeepEqual(tindex.qauMetrics, wants) {
		t.Errorf("qauMetrics:\n%swants:\n%s", stringifyQuotaPoints(tindex.qauMetrics), stringifyQuotaPoints(wants))
	}
}
