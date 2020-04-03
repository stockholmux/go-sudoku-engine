package main

import (
	"fmt"
	"github.com/go-redis/redis/v7"
	"os"
	"os/signal"
	"sudoku_engine/internal"
	"syscall"
)

// Settings about MatchmakingService (must be in sync)
const MatchmakingKeyPrefix = "mm:matches"
const MatchmakingKeyPartitions = 3

// Namespace all our keys under this prefix
const KeyNS = "sudoku-engine:"

func main() {

	if len(os.Args) != 2 {
		fmt.Println("Usage: ./mm_sync <uniqueInstanceName>")
		os.Exit(1)
	}

	instanceID := os.Args[1]

	// NOTE: the pool needs to be larger than the number of partitions
	//       we use for public streams
	redisClient := redis.NewClient(&redis.Options{Addr: "localhost:6379", PoolSize: 10})
	sudokuEngine := internal.NewSudokuEngine(KeyNS, redisClient, func(game internal.SudokuBoard) {})
	sudokuEngine.StartMatchmakingObserver(instanceID, MatchmakingKeyPrefix, MatchmakingKeyPartitions)

	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	fmt.Println("Ready!\n")
	<-c
	fmt.Println("Bye!")
}
