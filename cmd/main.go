package main

import (
	"encoding/json"
	"github.com/go-redis/redis/v7"
	"github.com/gorilla/websocket"
	"log"
	"net/http"
	"strconv"
	"sudoku_engine/internal"
)

// Namespace all our keys under this prefix
const KeyNS = "sudoku-engine:"

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

func main() {
	addr := "localhost:8082"
	redisClient := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	gameClients := NewWSDispatch()
	sudokuEngine := internal.NewSudokuEngine(KeyNS, redisClient, gameClients.SendUpdate)

	http.HandleFunc("/get_game", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")

		q := r.URL.Query()
		mid := q["mid"]

		sudokuEngine.EnsureMatchExists(mid[0])
		game, err := sudokuEngine.GetGame(mid[0])
		if err != nil {
			panic(err)
		}

		js, err := json.Marshal(game)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Write(js)
	})

	http.HandleFunc("/make_move", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")

		q := r.URL.Query()
		mid, row, col, number := q["mid"], q["row"], q["col"], q["number"]

		row_num, err := strconv.Atoi(row[0])
		if err != nil {
			panic(err)
		}

		col_num, err := strconv.Atoi(col[0])
		if err != nil {
			panic(err)
		}

		if err := sudokuEngine.MakeMove(mid[0], row_num, col_num, number[0][0]); err != nil {
			panic(err)
		}

		w.Write([]byte("{ \"status\" : \"OK\" }"))

	})

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		mid := q["mid"]
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Println(err)
			return
		}

		conn.SetPongHandler(func(string) error { return nil })

		gameClients.AddListener(mid[0], conn)
	})

	// Start broadcasting game updates
	go sudokuEngine.Start()
	go gameClients.Start()

	// Serve HTTP
	err := http.ListenAndServe(addr, nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}

// Websockets

type addListenerMsg struct {
	mid  string
	conn *websocket.Conn
}

type wsdispatch struct {
	listeners     map[string][]*websocket.Conn
	addListenerCh chan addListenerMsg
	updatesCh     chan internal.SudokuBoard
}

func NewWSDispatch() wsdispatch {
	return wsdispatch{
		listeners:     make(map[string][]*websocket.Conn),
		addListenerCh: make(chan addListenerMsg, 10),
		updatesCh:     make(chan internal.SudokuBoard, 10),
	}
}

func (w *wsdispatch) AddListener(mid string, conn *websocket.Conn) {
	w.addListenerCh <- addListenerMsg{mid, conn}
}

func (w *wsdispatch) SendUpdate(game internal.SudokuBoard) {
	w.updatesCh <- game
}

func (w *wsdispatch) Start() {
	for {
		select {
		case req := <-w.addListenerCh:
			w.listeners[req.mid] = append(w.listeners[req.mid], req.conn)
		case req := <-w.updatesCh:
			for _, ws := range w.listeners[req.Mid] {
				ws.WriteJSON(req)
			}
		}
	}
}
