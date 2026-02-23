// Command benchreport runs Grove ORM benchmarks and generates markdown reports.
//
// Usage:
//
//	go run ./cmd/benchreport
//	go run ./cmd/benchreport --update
//	go run ./cmd/benchreport --count 3 --timeout 5m
package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"math"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

// benchResult holds a single parsed benchmark measurement.
type benchResult struct {
	Category string
	Variant  string
	NsOp     float64
	BOp      float64
	AllocsOp float64
}

// avgResult holds averaged benchmark measurements for a category/variant pair.
type avgResult struct {
	Category string
	Variant  string
	NsOp     float64
	BOp      float64
	AllocsOp float64
}

// categoryOrder defines the display order for benchmark categories.
var categoryOrder = []string{
	"Insert",
	"SelectOne",
	"SelectMulti",
	"Update",
	"Delete",
	"BulkInsert100",
	"BulkInsert1000",
	"BuildSelect",
	"BuildInsert",
	"BuildUpdate",
	"SchemaCache",
	"TagResolution",
}

// benchLineRe matches Go benchmark output lines.
// Example: BenchmarkInsert/RawSQL-10         	  123456	      5230 ns/op	     312 B/op	       8 allocs/op
var benchLineRe = regexp.MustCompile(
	`^Benchmark([^/]+)/([^-\s]+)-\d+\s+\d+\s+([\d.]+)\s+ns/op\s+([\d.]+)\s+B/op\s+([\d.]+)\s+allocs/op`,
)

func main() {
	update := flag.Bool("update", false, "Update README.md and docs/content/docs/concepts/benchmarks.mdx with benchmark results")
	count := flag.Int("count", 5, "Number of benchmark runs")
	timeout := flag.String("timeout", "10m", "Benchmark timeout duration")
	flag.Parse()

	// Run benchmarks.
	results, err := runBenchmarks(*count, *timeout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error running benchmarks: %v\n", err)
		os.Exit(1)
	}

	if len(results) == 0 {
		fmt.Fprintf(os.Stderr, "No benchmark results parsed from output.\n")
		os.Exit(1)
	}

	// Average results across runs.
	averaged := averageResults(results)

	// Group by category.
	groups := groupByCategory(averaged)

	// Generate markdown.
	md := generateMarkdown(groups, *count)

	if *update {
		// Update README.md
		if err := updateFile("../README.md", "<!-- BENCH:START -->", "<!-- BENCH:END -->", md); err != nil {
			fmt.Fprintf(os.Stderr, "Error updating README.md: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("Updated README.md")

		// Update benchmarks.mdx
		mdxPath := "../docs/content/docs/concepts/benchmarks.mdx"
		if err := updateFile(mdxPath, "{/* BENCH:START */}", "{/* BENCH:END */}", md); err != nil {
			fmt.Fprintf(os.Stderr, "Error updating %s: %v\n", mdxPath, err)
			os.Exit(1)
		}
		fmt.Printf("Updated %s\n", mdxPath)
	} else {
		fmt.Print(md)
	}
}

// runBenchmarks executes go test with benchmark flags and parses the output.
func runBenchmarks(count int, timeout string) ([]benchResult, error) {
	args := []string{
		"test",
		"-bench=.",
		"-benchmem",
		fmt.Sprintf("-count=%d", count),
		fmt.Sprintf("-timeout=%s", timeout),
		"./...",
	}

	cmd := exec.Command("go", args...)
	cmd.Dir = "."

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	fmt.Fprintf(os.Stderr, "Running: go %s\n", strings.Join(args, " "))

	if err := cmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "go test stderr:\n%s\n", stderr.String())
		return nil, fmt.Errorf("go test failed: %w", err)
	}

	return parseBenchOutput(stdout.String()), nil
}

// parseBenchOutput parses Go benchmark output lines into benchResult slices.
func parseBenchOutput(output string) []benchResult {
	var results []benchResult
	scanner := bufio.NewScanner(strings.NewReader(output))
	for scanner.Scan() {
		line := scanner.Text()
		matches := benchLineRe.FindStringSubmatch(line)
		if matches == nil {
			continue
		}

		category := matches[1]
		variant := matches[2]
		nsOp, _ := strconv.ParseFloat(matches[3], 64)
		bOp, _ := strconv.ParseFloat(matches[4], 64)
		allocsOp, _ := strconv.ParseFloat(matches[5], 64)

		results = append(results, benchResult{
			Category: category,
			Variant:  variant,
			NsOp:     nsOp,
			BOp:      bOp,
			AllocsOp: allocsOp,
		})
	}
	return results
}

// averageResults averages benchmark results across multiple runs for each
// unique category/variant pair.
func averageResults(results []benchResult) []avgResult {
	type key struct {
		Category string
		Variant  string
	}
	type accumulator struct {
		NsOp     float64
		BOp      float64
		AllocsOp float64
		Count    int
	}

	accum := make(map[key]*accumulator)
	// Track insertion order so output is deterministic within categories.
	var order []key

	for _, r := range results {
		k := key{Category: r.Category, Variant: r.Variant}
		if _, ok := accum[k]; !ok {
			accum[k] = &accumulator{}
			order = append(order, k)
		}
		a := accum[k]
		a.NsOp += r.NsOp
		a.BOp += r.BOp
		a.AllocsOp += r.AllocsOp
		a.Count++
	}

	averaged := make([]avgResult, 0, len(order))
	for _, k := range order {
		a := accum[k]
		averaged = append(averaged, avgResult{
			Category: k.Category,
			Variant:  k.Variant,
			NsOp:     a.NsOp / float64(a.Count),
			BOp:      a.BOp / float64(a.Count),
			AllocsOp: a.AllocsOp / float64(a.Count),
		})
	}
	return averaged
}

// groupByCategory groups averaged results by category, preserving variant order
// within each category.
func groupByCategory(results []avgResult) map[string][]avgResult {
	groups := make(map[string][]avgResult)
	for _, r := range results {
		groups[r.Category] = append(groups[r.Category], r)
	}
	return groups
}

// hasRawSQL returns true if the category group contains a RawSQL variant.
func hasRawSQL(variants []avgResult) bool {
	for _, v := range variants {
		if v.Variant == "RawSQL" {
			return true
		}
	}
	return false
}

// getRawSQLNsOp returns the ns/op value for the RawSQL variant in a group.
func getRawSQLNsOp(variants []avgResult) float64 {
	for _, v := range variants {
		if v.Variant == "RawSQL" {
			return v.NsOp
		}
	}
	return 0
}

// formatNumber formats a float64 as a comma-separated integer string.
func formatNumber(f float64) string {
	n := int64(math.Round(f))
	if n < 0 {
		return fmt.Sprintf("-%s", formatPositiveInt(-n))
	}
	return formatPositiveInt(n)
}

// formatPositiveInt formats a non-negative integer with comma separators.
func formatPositiveInt(n int64) string {
	if n < 1000 {
		return strconv.FormatInt(n, 10)
	}

	s := strconv.FormatInt(n, 10)
	var buf strings.Builder
	remainder := len(s) % 3
	if remainder > 0 {
		buf.WriteString(s[:remainder])
		if len(s) > remainder {
			_ = buf.WriteByte(',')
		}
	}
	for i := remainder; i < len(s); i += 3 {
		if i > remainder {
			_ = buf.WriteByte(',')
		}
		buf.WriteString(s[i : i+3])
	}
	return buf.String()
}

// formatOverhead formats the overhead percentage compared to a baseline.
func formatOverhead(nsOp, baselineNsOp float64) string {
	if baselineNsOp == 0 {
		return "N/A"
	}
	pct := ((nsOp - baselineNsOp) / baselineNsOp) * 100.0
	if pct < 0 {
		return fmt.Sprintf("%.1f%%", pct)
	}
	return fmt.Sprintf("+%.1f%%", pct)
}

// variantDisplayName returns a human-friendly display name for a variant.
func variantDisplayName(variant string) string {
	switch variant {
	case "RawSQL":
		return "Raw SQL"
	default:
		return variant
	}
}

// generateMarkdown builds the complete markdown report from grouped results.
func generateMarkdown(groups map[string][]avgResult, count int) string {
	var buf strings.Builder

	goVersion := runtime.Version()
	osArch := fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH)
	dateStamp := time.Now().Format("2006-01-02")

	fmt.Fprintf(&buf,
		"> Benchmarks generated on %s with %s on %s. Each benchmark ran %d times; values are averages.\n",
		dateStamp, goVersion, osArch, count,
	)

	for _, category := range categoryOrder {
		variants, ok := groups[category]
		if !ok {
			continue
		}

		fmt.Fprintf(&buf, "\n### %s\n\n", category)

		if hasRawSQL(variants) {
			baselineNsOp := getRawSQLNsOp(variants)

			buf.WriteString("| Library | ns/op | B/op | allocs/op | vs Raw SQL |\n")
			buf.WriteString("|---------|------:|-----:|----------:|-----------:|\n")

			// Sort variants: RawSQL first, then by ns/op ascending.
			sorted := make([]avgResult, len(variants))
			copy(sorted, variants)
			sort.SliceStable(sorted, func(i, j int) bool {
				if sorted[i].Variant == "RawSQL" {
					return true
				}
				if sorted[j].Variant == "RawSQL" {
					return false
				}
				return sorted[i].NsOp < sorted[j].NsOp
			})

			for _, v := range sorted {
				overhead := "baseline"
				if v.Variant != "RawSQL" {
					overhead = formatOverhead(v.NsOp, baselineNsOp)
				}
				fmt.Fprintf(&buf, "| %s | %s | %s | %d | %s |\n",
					variantDisplayName(v.Variant),
					formatNumber(v.NsOp),
					formatNumber(v.BOp),
					int64(math.Round(v.AllocsOp)),
					overhead,
				)
			}
		} else {
			buf.WriteString("| Variant | ns/op | B/op | allocs/op |\n")
			buf.WriteString("|---------|------:|-----:|----------:|\n")

			// Sort by ns/op ascending.
			sorted := make([]avgResult, len(variants))
			copy(sorted, variants)
			sort.SliceStable(sorted, func(i, j int) bool {
				return sorted[i].NsOp < sorted[j].NsOp
			})

			for _, v := range sorted {
				fmt.Fprintf(&buf, "| %s | %s | %s | %d |\n",
					variantDisplayName(v.Variant),
					formatNumber(v.NsOp),
					formatNumber(v.BOp),
					int64(math.Round(v.AllocsOp)),
				)
			}
		}
	}

	// Handle any categories not in categoryOrder (append at the end).
	var extras []string
	knownCategories := make(map[string]bool)
	for _, c := range categoryOrder {
		knownCategories[c] = true
	}
	for category := range groups {
		if !knownCategories[category] {
			extras = append(extras, category)
		}
	}
	sort.Strings(extras)

	for _, category := range extras {
		variants := groups[category]

		fmt.Fprintf(&buf, "\n### %s\n\n", category)

		if hasRawSQL(variants) {
			baselineNsOp := getRawSQLNsOp(variants)

			buf.WriteString("| Library | ns/op | B/op | allocs/op | vs Raw SQL |\n")
			buf.WriteString("|---------|------:|-----:|----------:|-----------:|\n")

			sorted := make([]avgResult, len(variants))
			copy(sorted, variants)
			sort.SliceStable(sorted, func(i, j int) bool {
				if sorted[i].Variant == "RawSQL" {
					return true
				}
				if sorted[j].Variant == "RawSQL" {
					return false
				}
				return sorted[i].NsOp < sorted[j].NsOp
			})

			for _, v := range sorted {
				overhead := "baseline"
				if v.Variant != "RawSQL" {
					overhead = formatOverhead(v.NsOp, baselineNsOp)
				}
				fmt.Fprintf(&buf, "| %s | %s | %s | %d | %s |\n",
					variantDisplayName(v.Variant),
					formatNumber(v.NsOp),
					formatNumber(v.BOp),
					int64(math.Round(v.AllocsOp)),
					overhead,
				)
			}
		} else {
			buf.WriteString("| Variant | ns/op | B/op | allocs/op |\n")
			buf.WriteString("|---------|------:|-----:|----------:|\n")

			sorted := make([]avgResult, len(variants))
			copy(sorted, variants)
			sort.SliceStable(sorted, func(i, j int) bool {
				return sorted[i].NsOp < sorted[j].NsOp
			})

			for _, v := range sorted {
				fmt.Fprintf(&buf, "| %s | %s | %s | %d |\n",
					variantDisplayName(v.Variant),
					formatNumber(v.NsOp),
					formatNumber(v.BOp),
					int64(math.Round(v.AllocsOp)),
				)
			}
		}
	}

	return buf.String()
}

// updateFile reads a file, replaces content between start and end markers,
// and writes the file back.
func updateFile(path, startMarker, endMarker, content string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("reading %s: %w", path, err)
	}

	text := string(data)

	startIdx := strings.Index(text, startMarker)
	if startIdx == -1 {
		return fmt.Errorf("start marker %q not found in %s", startMarker, path)
	}

	endIdx := strings.Index(text, endMarker)
	if endIdx == -1 {
		return fmt.Errorf("end marker %q not found in %s", endMarker, path)
	}

	if endIdx <= startIdx {
		return fmt.Errorf("end marker appears before start marker in %s", path)
	}

	// Build the new file content: everything before the start marker line end,
	// then the new content, then the end marker and everything after.
	var result strings.Builder
	result.WriteString(text[:startIdx+len(startMarker)])
	result.WriteString("\n")
	result.WriteString(content)
	result.WriteString("\n")
	result.WriteString(text[endIdx:])

	if err := os.WriteFile(path, []byte(result.String()), 0644); err != nil {
		return fmt.Errorf("writing %s: %w", path, err)
	}

	return nil
}
