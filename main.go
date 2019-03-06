package main

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/urfave/cli"
	"google.golang.org/grpc"

	snapshotsapi "github.com/containerd/containerd/api/services/snapshots/v1"
	"github.com/containerd/containerd/contrib/snapshotservice"
	lvms "github.com/ganeshmaharaj/lvm-snapshotter/lvmsnapshotter"
)

var usage = fmt.Sprint(`an image layering tool using the containerd shapshots
	 API and the thin logical volume device mapper of LVM.

	 To run lvm-snapshotter a path to the unix socket address, volume group name,
	 and logical volume pool name are required:

	 $ lvm-snapshotter --addr /path/to/socket --vgname volumegroup --lvpoolname poolname
`)

func prepareSnapshotter(addr, vgname, lvpoolname string) error {
	// Create a gRPC server
	rpc := grpc.NewServer()

	// Configure your custom snapshotter, this example uses the native
	// snapshotter and a root directory. Your custom snapshotter will be
	// much more useful than using a snapshotter which is already included.
	// https://godoc.org/github.com/containerd/containerd/snapshots#Snapshotter
	sn, err := lvms.NewSnapshotter(vgname, lvpoolname)
	if err != nil {
		return err
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
	l, err := net.Listen("unix", addr)
	if err != nil {
		return err
	}
	if err := rpc.Serve(l); err != nil {
		return err
	}
	return nil
}

func createApp() error {
	var addr string
	var vgname string
	var lvpoolname string

	app := cli.NewApp()
	app.Name = "lvmsnapshotter"
	app.Usage = usage
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:        "addr",
			Usage:       "the socket the snapshotter will listen on, it is the 'address' in the containerd 'proxy plugin' configuration.",
			Destination: &addr,
		},
		cli.StringFlag{
			Name:        "vgname",
			Usage:       "name of created volume group",
			Destination: &vgname,
		},
		cli.StringFlag{
			Name:        "lvpoolname",
			Usage:       "name of logical volume pool",
			Destination: &lvpoolname,
		},
	}
	app.Action = func(ctx *cli.Context) error {
		if ctx.NumFlags() != 3 {
			return fmt.Errorf("incorrect usage, view help for correct argument usage")
		}
		return prepareSnapshotter(addr, vgname, lvpoolname)
	}
	return app.Run(os.Args)
}

func main() {
	if err := createApp(); err != nil {
		fmt.Println("error:", err)
		os.Exit(1)
	}
}
