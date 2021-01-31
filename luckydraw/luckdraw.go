package luckydraw

import (
	"crypto/md5"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Participant struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type Prize struct {
	No     int    `json:"no"`
	Name   string `json:"name"`
	Amount int    `json:"amount"`
	Desc   string `json:"desc"`
}

type Lottery struct {
	name         string
	prizes       map[int]Prize
	participants map[string]Participant
	winners      map[int][]Participant
	mutex        *sync.Mutex
}

type SaveData struct {
	Name         string                 `json:"name"`
	Prizes       map[int]Prize          `json:"prizes"`
	Participants map[string]Participant `json:"participants"`
	Winners      map[int][]Participant  `json:"winners"`
	LastUpdated  string                 `json:"last_updated"`
	Checksum     string                 `json:"checksum"`
}

var (
	ErrParticipantsCSV               = fmt.Errorf("incorrect participants CSV")
	ErrPrizeNo                       = fmt.Errorf("incorrect prize no")
	ErrWinnersExistBeforeDraw        = fmt.Errorf("winners exist before draw")
	ErrPrizeAmount                   = fmt.Errorf("incorrect prize amount")
	ErrNoAvailableParticipants       = fmt.Errorf("no available participants")
	ErrNoOriginalWinnersBeforeRedraw = fmt.Errorf("no original winners before redraw")
	ErrRevokedWinnerNotMatch         = fmt.Errorf("revoked winner does not match")
	ErrWinnersNotExistBeforeReDraw   = fmt.Errorf("winners don't exist before redraw")
	ErrRedrawPrizeAmount             = fmt.Errorf("incorrect redraw prize amount")
	ErrChecksum                      = fmt.Errorf("incorrect checksum")
	AppDataDir                       string
)

func init() {
}

func New(name string) *Lottery {
	l := &Lottery{
		name,
		make(map[int]Prize),
		make(map[string]Participant),
		make(map[int][]Participant),
		&sync.Mutex{},
	}

	return l
}

func (l *Lottery) SetPrize(no int, name string, amount int, desc string) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	prize := Prize{no, name, amount, desc}
	l.prizes[no] = prize
}

func (l *Lottery) Prize(no int) Prize {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	return l.prizes[no]
}

func (l *Lottery) LoadPrizesCSV(r io.Reader) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	reader := csv.NewReader(r)
	rows, err := reader.ReadAll()
	if err != nil {
		return err
	}

	l.prizes = make(map[int]Prize)
	for i := 1; i < len(rows); i++ {
		row := rows[i]

		if len(row) != 4 {
			return ErrParticipantsCSV
		}
		no, err := strconv.Atoi(strings.Trim(row[0], " "))
		if err != nil {
			return err
		}
		name := row[1]
		amount, err := strconv.Atoi(strings.Trim(row[2], " "))
		if err != nil {
			return err
		}
		desc := row[3]

		l.prizes[no] = Prize{no, name, amount, desc}
	}
	return nil
}

func (l *Lottery) LoadPrizesCSVFile(file string) error {
	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()

	return l.LoadPrizesCSV(f)
}

func prizeMapToSlice(m map[int]Prize, descOrder bool) []Prize {
	s := []int{}
	prizes := []Prize{}

	// Sort prize map by key
	for prizeNo, _ := range m {
		s = append(s, prizeNo)
	}

	sort.Slice(s, func(i, j int) bool {
		if descOrder {
			return s[i] > s[j]
		} else {
			return s[i] < s[j]
		}
	})

	for _, prizeNo := range s {
		prizes = append(prizes, m[prizeNo])
	}

	return prizes
}

func (l *Lottery) Prizes(descOrder bool) []Prize {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	return prizeMapToSlice(l.prizes, descOrder)
}

func (l *Lottery) LoadParticipantsCSV(r io.Reader) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	reader := csv.NewReader(r)
	rows, err := reader.ReadAll()
	if err != nil {
		return err
	}

	l.participants = make(map[string]Participant)
	for i := 1; i < len(rows); i++ {
		row := rows[i]
		if len(row) != 2 {
			return ErrParticipantsCSV
		}
		ID := row[0]
		name := row[1]
		l.participants[ID] = Participant{ID, name}
	}
	return nil
}

func (l *Lottery) LoadParticipantsCSVFile(file string) error {
	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()

	return l.LoadParticipantsCSV(f)
}

func participantMapToSlice(m map[string]Participant) []Participant {
	participants := []Participant{}

	for _, p := range m {
		participants = append(participants, p)
	}

	return participants
}

func participantSliceToMap(s []Participant) map[string]Participant {
	m := make(map[string]Participant)

	for _, p := range s {
		m[p.ID] = p
	}

	return m
}

func (l *Lottery) Participants() []Participant {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	return participantMapToSlice(l.participants)
}

func copyParticipantMap(m map[string]Participant) map[string]Participant {
	copiedMap := make(map[string]Participant)

	for k, v := range m {
		copiedMap[k] = v
	}

	return copiedMap
}

func (l *Lottery) availableParticipants(prizeNo int) []Participant {
	participants := copyParticipantMap(l.participants)

	// Remove winners
	for _, winners := range l.winners {
		for _, winner := range winners {
			delete(participants, winner.ID)
		}
	}

	return participantMapToSlice(participants)
}

func (l *Lottery) AvailableParticipants(prizeNo int) []Participant {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	return l.availableParticipants(prizeNo)
}

func (l *Lottery) Winners(prizeNo int) []Participant {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if _, ok := l.winners[prizeNo]; !ok {
		return []Participant{}
	}

	return l.winners[prizeNo]
}

func removeParticipant(s []Participant, i int) []Participant {
	l := len(s)
	if l <= 0 {
		return s
	}

	if i < 0 || i > l-1 {
		return s
	}

	s[i] = s[l-1]
	return s[:l-1]
}

func draw(prizeAmount int, participants []Participant) []Participant {
	winners := []Participant{}

	if prizeAmount <= 0 || len(participants) <= 0 {
		return winners
	}

	// Check prize amount.
	amount := prizeAmount
	// If participants amount < prize amount,
	// use participants amount as the new prize amount.
	if len(participants) < prizeAmount {
		amount = len(participants)
	}

	for i := 0; i < amount; i++ {
		rand.Seed(time.Now().UnixNano())
		index := rand.Intn(len(participants))
		winners = append(winners, participants[index])
		participants = removeParticipant(participants, index)
	}

	return winners
}

func (l *Lottery) Draw(prizeNo int) ([]Participant, error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	winners := []Participant{}

	if _, ok := l.prizes[prizeNo]; !ok {
		return winners, ErrPrizeNo
	}

	amount := l.prizes[prizeNo].Amount
	if amount < 1 {
		return winners, ErrPrizeAmount
	}

	if _, ok := l.winners[prizeNo]; ok {
		return winners, ErrWinnersExistBeforeDraw
	}

	participants := l.availableParticipants(prizeNo)
	if len(participants) == 0 {
		return winners, ErrNoAvailableParticipants
	}

	winners = draw(amount, participants)

	l.winners[prizeNo] = winners
	return winners, nil
}

// Revoke revokes the winners of the given prize.
// It'll remove revoked winners from winners of the prize.
func (l *Lottery) Revoke(prizeNo int, revokedWinners []Participant) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if _, ok := l.prizes[prizeNo]; !ok {
		return ErrPrizeNo
	}

	amount := l.prizes[prizeNo].Amount
	if amount < 1 {
		return ErrPrizeAmount
	}

	if _, ok := l.winners[prizeNo]; !ok {
		return ErrNoOriginalWinnersBeforeRedraw
	}

	// Remove original winners for the prize before re-draw.
	originalWinnerMap := participantSliceToMap(l.winners[prizeNo])

	for _, revokedWinner := range revokedWinners {
		if _, ok := originalWinnerMap[revokedWinner.ID]; !ok {
			return ErrRevokedWinnerNotMatch
		}
		delete(originalWinnerMap, revokedWinner.ID)
	}

	l.winners[prizeNo] = participantMapToSlice(originalWinnerMap)
	return nil
}

func (l *Lottery) Redraw(prizeNo int, amount int) ([]Participant, error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	winners := []Participant{}

	if _, ok := l.prizes[prizeNo]; !ok {
		return winners, ErrPrizeNo
	}

	if l.prizes[prizeNo].Amount < 1 {
		return winners, ErrPrizeAmount
	}

	if _, ok := l.winners[prizeNo]; !ok {
		return winners, ErrWinnersNotExistBeforeReDraw
	}

	if amount > l.prizes[prizeNo].Amount-len(l.winners[prizeNo]) {
		return winners, ErrRedrawPrizeAmount
	}

	participants := l.availableParticipants(prizeNo)
	if len(participants) == 0 {
		return winners, ErrNoAvailableParticipants
	}

	// Get new winners.
	winners = draw(amount, participants)

	// Append new winners and original winners.
	l.winners[prizeNo] = append(l.winners[prizeNo], winners...)
	return winners, nil
}

func (l *Lottery) AllWinners() map[int][]Participant {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	return l.winners
}

func (l *Lottery) ClearWinners(prizeNo int) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	// Clear the winner slice.
	l.winners[prizeNo] = []Participant{}
}

func (l *Lottery) ClearAllWinners() {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	l.winners = make(map[int][]Participant)
}

func makeDataFileName(name string) string {
	f := fmt.Sprintf("%X.json", md5.Sum([]byte(name)))
	return path.Join(AppDataDir, f)
}

func computeWinnersHash(winners map[int][]Participant) []byte {
	var arr []int

	// Sort winner map by key
	for prizeNo, _ := range winners {
		arr = append(arr, prizeNo)
	}

	sort.Slice(arr, func(i, j int) bool {
		return arr[i] < arr[j]
	})

	h := md5.New()
	for _, prizeNo := range arr {
		s := strconv.FormatInt(int64(prizeNo), 10)
		h.Write([]byte(s))
		for _, winner := range winners[prizeNo] {
			h.Write([]byte(winner.ID))
			h.Write([]byte(winner.Name))
		}
	}

	return h.Sum(nil)
}

func (l *Lottery) Save(w io.Writer) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	tm := time.Now()

	data := SaveData{
		l.name,
		l.prizes,
		l.participants,
		l.winners,
		fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d",
			tm.Year(),
			tm.Month(),
			tm.Day(),
			tm.Hour(),
			tm.Minute(),
			tm.Second(),
		),
		fmt.Sprintf("%X", computeWinnersHash(l.winners)),
	}

	enc := json.NewEncoder(w)
	enc.SetIndent("", "    ")
	return enc.Encode(&data)
}

func (l *Lottery) SaveToFile() error {
	dataFile := makeDataFileName(l.name)

	f, err := os.Create(dataFile)
	if err != nil {
		return err
	}
	defer f.Close()

	return l.Save(f)
}

func (l *Lottery) Load(r io.Reader) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	data := SaveData{}
	dec := json.NewDecoder(r)

	if err := dec.Decode(&data); err != nil {
		return err
	}

	checksum := computeWinnersHash(data.Winners)
	if fmt.Sprintf("%X", checksum) != data.Checksum {
		return ErrChecksum
	}

	l.prizes = data.Prizes
	l.participants = data.Participants
	l.winners = data.Winners

	// Check if map is nil
	if l.prizes == nil {
		l.prizes = make(map[int]Prize)
	}

	if l.participants == nil {
		l.participants = make(map[string]Participant)
	}

	if l.winners == nil {
		l.winners = make(map[int][]Participant)
	}

	return nil
}

func (l *Lottery) LoadFromFile() error {
	dataFile := makeDataFileName(l.name)

	f, err := os.Open(dataFile)
	if err != nil {
		return err
	}
	defer f.Close()

	return l.Load(f)
}

func (l *Lottery) DataFileExists() bool {
	dataFile := makeDataFileName(l.name)

	if _, err := os.Stat(dataFile); os.IsNotExist(err) {
		return false
	}
	return true
}
