package traceenrichment

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"sync"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processorhelper"
	"go.uber.org/zap"
)

const (
	// Имена колонок в CSV-файле
	traceCodeHeader = "traceCode"
	slugHeader      = "slug"
)

// Config содержит конфигурацию процессора
type Config struct {
	CSVFilePath string `mapstructure:"csv_file_path"`
}

// Processor реализует обогащение трассировки
type Processor struct {
	logger        *zap.Logger
	traceCodeToSlug map[string]string
	slugToTraceCode map[string]string
	mutex         sync.RWMutex
}

// NewProcessor создает новый экземпляр процессора
func NewProcessor(logger *zap.Logger, config *Config) (*Processor, error) {
	p := &Processor{
		logger:        logger,
		traceCodeToSlug: make(map[string]string),
		slugToTraceCode: make(map[string]string),
	}

	if err := p.loadCSVData(config.CSVFilePath); err != nil {
		return nil, err
	}

	return p, nil
}

// loadCSVData загружает данные из CSV-файла
func (p *Processor) loadCSVData(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open CSV file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)

	// Чтение заголовков
	headers, err := reader.Read()
	if err != nil {
		return fmt.Errorf("failed to read CSV headers: %w", err)
	}

	// Определение индексов колонок
	traceCodeIdx := -1
	slugIdx := -1
	for i, header := range headers {
		switch header {
		case traceCodeHeader:
			traceCodeIdx = i
		case slugHeader:
			slugIdx = i
		}
	}

	if traceCodeIdx == -1 || slugIdx == -1 {
		return fmt.Errorf("required headers not found in CSV file")
	}

	// Чтение данных
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading CSV record: %w", err)
		}

		traceCode := record[traceCodeIdx]
		slug := record[slugIdx]

		if traceCode != "" && slug != "" {
			p.traceCodeToSlug[traceCode] = slug
			p.slugToTraceCode[slug] = traceCode
		}
	}

	p.logger.Info("CSV data loaded successfully",
		zap.Int("trace_code_entries", len(p.traceCodeToSlug)),
		zap.Int("slug_entries", len(p.slugToTraceCode)))

	return nil
}

// ProcessTraces обрабатывает трассировку
func (p *Processor) ProcessTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		ilss := rs.ScopeSpans()
		for j := 0; j < ilss.Len(); j++ {
			ils := ilss.At(j)
			spans := ils.Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				p.enrichSpan(span.Attributes())
			}
		}
	}
	return td, nil
}

// enrichSpan обогащает span атрибутами
func (p *Processor) enrichSpan(attributes pcommon.Map) {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	// Проверка наличия traceCode и обогащение slug
	if traceCodeVal, exists := attributes.Get(traceCodeHeader); exists {
		traceCode := traceCodeVal.AsString()
		if slug, ok := p.traceCodeToSlug[traceCode]; ok && !attributes.Contains(slugHeader) {
			attributes.PutStr(slugHeader, slug)
		}
	}

	// Проверка наличия slug и обогащение traceCode
	if slugVal, exists := attributes.Get(slugHeader); exists {
		slug := slugVal.AsString()
		if traceCode, ok := p.slugToTraceCode[slug]; ok && !attributes.Contains(traceCodeHeader) {
			attributes.PutStr(traceCodeHeader, traceCode)
		}
	}
}

// CreateTracesProcessor создает процессор трассировки
func CreateTracesProcessor(
	ctx context.Context,
	set component.ProcessorCreateSettings,
	cfg component.Config,
	nextConsumer component.TracesConsumer,
) (component.TracesProcessor, error) {
	pCfg := cfg.(*Config)
	processor, err := NewProcessor(set.Logger, pCfg)
	if err != nil {
		return nil, err
	}

	return processorhelper.NewTracesProcessor(
		ctx,
		set,
		cfg,
		nextConsumer,
		processor.ProcessTraces,
	)
}