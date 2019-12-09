package googlecloudfunctions

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/presslabs/gke-weekend-downscaler/downscaler"
)

const (
	OperationScaleUp   = "scale-up"
	OperationScaleDown = "scale-down"
)

type Options struct {
	Operation string   `json:"operation"`
	DryRun    bool     `json:"dryRun"`
	Match     []string `json:"matches"`
}

func (o *Options) Validate() error {
	if len(o.Match) == 0 {
		return fmt.Errorf("no matches specified")
	}
	switch o.Operation {
	case OperationScaleUp:
	case OperationScaleDown:
	default:
		return fmt.Errorf("invalid operation %s. Operation must be scale-up or scale-down.", o.Operation)
	}
	return nil
}

// PubSubMessage is the payload of a Pub/Sub event.
type PubSubMessage struct {
	Data []byte `json:"data"`
}

// HelloPubSub consumes a Pub/Sub message.
func Downscale(ctx context.Context, m PubSubMessage) error {
	opts := Options{}
	err := json.Unmarshal(m.Data, &opts)
	if err != nil {
		log.Printf("%v", err)
		return err
	}
	err = opts.Validate()
	if err != nil {
		log.Printf("%v", err)
		return err
	}
	w, err := downscaler.New(context.Background(), opts.Match, opts.DryRun)
	if err != nil {
		log.Fatal(err)
	}
	if err != nil {
		log.Printf("%v", err)
		return err
	}
	switch opts.Operation {
	case OperationScaleUp:
		err = w.ScaleUp()
	case OperationScaleDown:
		err = w.ScaleDown()
	}
	if err != nil {
		log.Printf("%v", err)
		return err
	}
	return nil
}
