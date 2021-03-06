package telegraf

import "context"

type Input interface {
    // SampleConfig returns the default configuration of the Input
    SampleConfig() string

    // Description returns a one-sentence description on the Input
    Description() string

    // Gather takes in an accumulator and adds the metrics that the Input
    // gathers. This is called every "interval"
    Gather(Accumulator) error
}

type ServiceInput interface {
    Input

    // Start the ServiceInput.  The Accumulator may be retained and used until
    // Stop returns.
    Start(Accumulator) error

    // Stop stops the services and closes any necessary channels and connections
    Stop()
}

//ServiceInputWithContext -- greatdolphin 20200520
type ServiceInputWithContext interface {
    Input

    // Start the ServiceInput.  The Accumulator may be retained and used until
    // Stop returns.
    // context is the invoker's context which is used to cancel ServiceInput by invoker
    StartContext(context.Context, Accumulator) error
    // Stop stops the services and closes any necessary channels and connections
    Stop()
}

