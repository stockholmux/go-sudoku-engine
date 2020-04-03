# go-sudoku-engine

A service written in Go to serve up Sudoku puzzels.

## Launch
`go run cmd/main.go`

## Connecting to websocket
Suggested tool: https://github.com/esphen/wsta

`./wsta "ws://localhost:8082/ws?mid=123"`

The `mid` parameter will create a new game under the given ID.


## Making a move
`http://localhost:8082/make_move?mid=123&row=8&col=8&number=6`

Rows and Cols are zero-indexed.
Passing a value for `number` otside the `0`-`9` range will free the cell.

Board updates are sent through the websocket connection.


## Ending conditions
The game concludes when the board is full and all values are correct.
This corresponds to the `"E"` (ended) state for the game.
