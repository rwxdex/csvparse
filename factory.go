package traceenrichment

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configmodels"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor/processorhelper"
)

const (
	// TypeStr - тип процессора
	TypeStr = "traceenrichment"
)

// NewFactory создает фабрику для процессора
func NewFactory() component.ProcessorFactory {
	return component.NewProcessorFactory(
		TypeStr,
		createDefaultConfig,
		component.WithTracesProcessor(createTracesProcessor))
}

func createDefaultConfig() component.Config {
	return &Config{
		CSVFilePath: "",
	}
}

func createTracesProcessor(
	ctx context.Context,
	params component.ProcessorCreateParams,
	cfg component.Config,
	nextConsumer consumer.Traces,
) (component.TracesProcessor, error) {
	return CreateTracesProcessor(ctx, params, cfg, nextConsumer)
}