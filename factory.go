package traceenrichment

import (
	"context"

	"go.opentelemetry.io/collector/component"
	// "go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor/processorhelper"
)

const (
	// TypeStr - тип процессора
	TypeStr = "traceenrichment"
)

// NewFactory создает фабрику для процессора
func NewFactory() component.ProcessorFactory {
	return processorhelper.NewProcessorFactory(
		TypeStr,
		createDefaultConfig,
		processorhelper.WithTracesProcessor(createTracesProcessor))
}

func createDefaultConfig() component.Config {
	return &Config{
		CSVFilePath: "",
	}
}

func createTracesProcessor(
	ctx context.Context,
	params processorhelper.ProcessorCreateParams,
	cfg component.Config,
	nextConsumer consumer.Traces,
) (processorhelper.TracesProcessor, error) {
	return CreateTracesProcessor(ctx, params, cfg, nextConsumer)
}