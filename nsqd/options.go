package nsqd

type Options struct {
	TCPAddress string `flag:"tcp-address"`
}

func NewOptions() *Options {
	return &Options{
		TCPAddress: "0.0.0.0:4150",
	}
}
