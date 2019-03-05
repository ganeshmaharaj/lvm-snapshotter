package main

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"

	snapshotsapi "github.com/containerd/containerd/api/services/snapshots/v1"
	"github.com/containerd/containerd/contrib/snapshotservice"
	lvms "github.com/ganeshmaharaj/lvm-snapshotter/lvmsnapshotter"
)

func main() {
	// Provide a unix address to listen to, this will be the `address`
	// in the `proxy_plugin` configuration.
	// The root will be used to store the snapshots.
	if len(os.Args) < 4 {
		fmt.Printf("invalid args: usage: %s <unix addr> <vgname> <lvpoolname>\n", os.Args[0])
		os.Exit(1)
	}

	// Create a gRPC server
	rpc := grpc.NewServer()

	// Configure your custom snapshotter, this example uses the native
	// snapshotter and a root directory. Your custom snapshotter will be
	// much more useful than using a snapshotter which is already included.
	// https://godoc.org/github.com/containerd/containerd/snapshots#Snapshotter
	sn, err := lvms.NewSnapshotter(os.Args[2], os.Args[3])
	if err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}
	defer sn.Close()

	// Convert the snapshotter to a gRPC service,
	// example in github.com/containerd/containerd/contrib/snapshotservice
	service := snapshotservice.FromSnapshotter(sn)

	// Register the service with the gRPC server
	snapshotsapi.RegisterSnapshotsServer(rpc, service)

	var gracefulstop = make(chan os.Signal)
	signal.Notify(gracefulstop, syscall.SIGTERM)
	signal.Notify(gracefulstop, syscall.SIGINT)
	signal.Notify(gracefulstop, syscall.SIGSTOP)
	go func() {
		<-gracefulstop
		rpc.GracefulStop()
		os.Remove(os.Args[1])
	}()

	// Listen and serve
	l, err := net.Listen("unix", os.Args[1])
	if err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}
	if err := rpc.Serve(l); err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}
}
