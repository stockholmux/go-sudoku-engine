package internal

import (
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v7"
	"github.com/kristoff-it/sudoku"
	"hash/crc32"
	"strconv"
	"time"
)

type engine struct {
	keyNS    string
	redis    *redis.Client
	updateFn func(game SudokuBoard)
}

type SudokuBoard struct {
	Mid  string
	Game map[string]interface{}
}

const STATE_ENDED = "E"
const SLOTS = 3

func NewSudokuEngine(keyNS string, r *redis.Client, updateFn func(game SudokuBoard)) engine {
	return engine{keyNS, r, updateFn}
}

func (e engine) GetGame(mid string) (map[string]interface{}, error) {
	board_key, _ := e.makeKeyName(mid)
	game, err := e.redis.HGetAll(board_key).Result()
	return map[string]interface{}{"Mid": mid, "Game": game}, err
}

func (e engine) MakeMove(mid string, row int, column int, number byte) error {
	board_key, slot := e.makeKeyName(mid)

	const retries = 2
	for i := 0; i < retries; i++ {
		txerr := e.redis.Watch(func(tx *redis.Tx) error {
			// Read the board state from Redis and use
			// optimistic locking to apply the update.
			var board sudoku.Puzzle
			board_hash, err := e.redis.HGetAll(board_key).Result()
			if err != nil {
				return err
			}

			// Don't do anything if the game is already over.
			if board_hash["state"] == STATE_ENDED {
				return nil
			}

			// Unmarshal the game board
			if err := board.Read([]byte(board_hash["puzzle"])); err != nil {
				return err
			}

			// Apply the move
			var original sudoku.Puzzle
			if err := original.Read([]byte(board_hash["original"])); err != nil {
				return err
			}

			if original.GetCell(sudoku.NewCellRef(row, column)) != ' ' {
				return nil
			}

			board.SetCell(sudoku.NewCellRef(row, column), number)
			board_hash["puzzle"] = board.String()

			// Is the game over?
			if board.Validate() == nil && board.NumUnknowns() == 0 {
				board_hash["state"] = STATE_ENDED
			}

			_, err = tx.Pipelined(func(pipe redis.Pipeliner) error {

				board_hash_if := map[string]interface{}{
					"puzzle": board_hash["puzzle"],
					"state":  board_hash["state"],
				}
				// Set the key (no need to rewrite 'original')
				pipe.HMSet(board_key, board_hash_if)

				// If the match has ended, publish that we're giving out some points
				if board_hash["state"] == STATE_ENDED {
					pipe.XAdd(&redis.XAddArgs{
						Stream: addSlot(e.keyNS+"games", slot),
						ID:     "*",
						Values: map[string]interface{}{
							"mid":   mid,
							"state": "ENDED",
						},
					})
				}

				// Propagate the update
				data, err := json.Marshal(SudokuBoard{mid, board_hash_if})
				if err != nil {
					return err
				}
				pipe.Publish(e.keyNS+"game-updates", data).Err()

				return nil
			})

			if err != nil {
				return err
			}

			return nil

		}, board_key)

		if txerr == redis.TxFailedErr {
			continue
		} else if txerr != nil {
			return txerr
		} else {
			break
		}
	}

	return nil
}

func (e engine) EnsureMatchExists(mid string) error {
	board_key, slot := e.makeKeyName(mid)

	exists, err := e.redis.Exists(board_key).Result()
	if err != nil {
		return err
	}

	if exists != 1 {
		// Generate the puzzle
		board := sudoku.GenerateSolution()
		fmt.Printf("Solution to [%s]:\n\n%s\n\n", mid, board.String())
		mask := board.MinimalMask()
		board = board.ApplyMask(&mask)

		if err := e.redis.HMSet(board_key, map[string]interface{}{
			"puzzle":   board.String(),
			"original": board.String(),
			"state":    "",
		}).Err(); err != nil {
			return err
		}
	}

	// Inform other services that the game is ready
	e.redis.XAdd(&redis.XAddArgs{
		Stream: addSlot(e.keyNS+"games", slot),
		ID:     "*",
		Values: map[string]interface{}{
			"mid":   mid,
			"state": "READY",
		},
	})
	return nil
}

func (e engine) Start() {
	// Subscribes to Pub/Sub to propagate updates
	pubsub := e.redis.Subscribe(e.keyNS + "game-updates")
	_, err := pubsub.Receive()
	if err != nil {
		panic(err)
	}

	// Send each update from Pub/Sub to the upper layer for delivery
	for msg := range pubsub.Channel() {
		var board SudokuBoard
		err := json.Unmarshal([]byte(msg.Payload), &board)
		if err != nil {
			panic(err)
		}
		e.updateFn(board)
	}
}

const GROUP_NAME = "sudoku-engine"

func (e engine) StartMatchmakingObserver(instanceID string, mmPublicStreamPrefix string, mmPartitions int) {
	for i := 0; i < mmPartitions; i += 1 {
		go func(idx string) {
			// Create the full name for this stream partition
			stream_name := addSlot(mmPublicStreamPrefix, idx)

			// Ensure existence of the consumer group
			_ = e.redis.XGroupCreateMkStream(stream_name, GROUP_NAME, "0").Err()

			// Read pending messages
			streams, err := e.redis.XReadGroup(&redis.XReadGroupArgs{
				Group:    GROUP_NAME,
				Consumer: instanceID,
				Streams:  []string{stream_name, "0"},
				Block:    60 * time.Second,
			}).Result()

			// Process messages
			for {

				if err != nil && err != redis.Nil {
					panic(err)
				} else {
					if err == nil {
						messages := streams[0].Messages
						ids_to_ack := make([]string, 10)
						for _, m := range messages {
							game := m.Values["game"].(string)
							match_id := m.Values["match_id"].(string)
							if game == "sudoku" {
								e.EnsureMatchExists(match_id)
							}

							ids_to_ack = append(ids_to_ack, m.ID)
						}

						e.redis.XAck(stream_name, GROUP_NAME, ids_to_ack...)
					}
				}

				// Ask for more messages
				streams, err = e.redis.XReadGroup(&redis.XReadGroupArgs{
					Group:    GROUP_NAME,
					Consumer: instanceID,
					Streams:  []string{stream_name, ">"},
					Count:    10,
					Block:    60 * time.Second,
				}).Result()

			}
		}(strconv.Itoa(i))
	}
}

func (e engine) makeKeyName(id string) (string, string) {
	crc32q := crc32.MakeTable(0xD5828281)
	slot := strconv.Itoa(int(crc32.Checksum([]byte(id), crc32q) % SLOTS))
	return addSlot(e.keyNS+id, slot), slot
}

func addSlot(key string, slot string) string {
	return key + "-{" + slot + "}"
}
