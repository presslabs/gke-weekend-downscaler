package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/spf13/cobra"

	"github.com/presslabs/gke-weekend-downscaler/downscaler"
)

var (
	globs  []string
	dryRun bool
)

func scaleDown(cmd *cobra.Command, args []string) {
	w, err := downscaler.New(context.Background(), globs, dryRun)
	if err != nil {
		log.Fatal(err)
	}
	err = w.ScaleDown()
	if err != nil {
		log.Fatal(err)
	}
}

func scaleUp(cmd *cobra.Command, args []string) {
	w, err := downscaler.New(context.Background(), globs, dryRun)
	if err != nil {
		log.Fatal(err)
	}

	err = w.ScaleUp()
	if err != nil {
		log.Fatal(err)
	}
}

func lsClusters(cmd *cobra.Command, args []string) {
	w, err := downscaler.New(context.Background(), globs, dryRun)
	if err != nil {
		log.Fatal(err)
	}

	err = w.ListClusters()
	if err != nil {
		log.Fatal(err)
	}
}

var rootCmd = &cobra.Command{
	Use:   "gke-weekend-downscaler",
	Short: "GKE Weekend Downscaler downscales GKE clusers on weekends",
}

var scaleUpCmd = &cobra.Command{
	Use:   "scale-up",
	Short: "Scale up clusters matching globs",
	Run:   scaleUp,
}

var scaleDownCmd = &cobra.Command{
	Use:   "scale-down",
	Short: "Scale down clusters matching globs",
	Run:   scaleDown,
}

var lsCmd = &cobra.Command{
	Use:   "ls",
	Short: "List clusters matching globs",
	Run:   lsClusters,
}

func main() {
	rootCmd.PersistentFlags().BoolVarP(&dryRun, "dry-run", "n", false, "don't change anything")
	rootCmd.PersistentFlags().StringSliceVarP(&globs, "match", "m", []string{}, "glob matching for clusters")
	rootCmd.AddCommand(scaleUpCmd)
	rootCmd.AddCommand(scaleDownCmd)
	rootCmd.AddCommand(lsCmd)
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
