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

type Draw struct {
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

func New(name string) *Draw {
	l := &Draw{
		name,
		make(map[int]Prize),
		make(map[string]Participant),
		make(map[int][]Participant),
		&sync.Mutex{},
	}

	return l
}

func (d *Draw) SetPrize(no int, name string, amount int, desc string) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	prize := Prize{no, name, amount, desc}
	d.prizes[no] = prize
}

func (d *Draw) Prize(no int) Prize {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	return d.prizes[no]
}

func (d *Draw) LoadPrizesCSV(r io.Reader) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	reader := csv.NewReader(r)
	rows, err := reader.ReadAll()
	if err != nil {
		return err
	}

	d.prizes = make(map[int]Prize)
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

		d.prizes[no] = Prize{no, name, amount, desc}
	}
	return nil
}

func (d *Draw) LoadPrizesCSVFile(file string) error {
	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()

	return d.LoadPrizesCSV(f)
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

func (d *Draw) Prizes(descOrder bool) []Prize {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	return prizeMapToSlice(d.prizes, descOrder)
}

func (d *Draw) LoadParticipantsCSV(r io.Reader) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	reader := csv.NewReader(r)
	rows, err := reader.ReadAll()
	if err != nil {
		return err
	}

	d.participants = make(map[string]Participant)
	for i := 1; i < len(rows); i++ {
		row := rows[i]
		if len(row) != 2 {
			return ErrParticipantsCSV
		}
		ID := row[0]
		name := row[1]
		d.participants[ID] = Participant{ID, name}
	}
	return nil
}

func (d *Draw) LoadParticipantsCSVFile(file string) error {
	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()

	return d.LoadParticipantsCSV(f)
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

func (d *Draw) Participants() []Participant {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	return participantMapToSlice(d.participants)
}

func copyParticipantMap(m map[string]Participant) map[string]Participant {
	copiedMap := make(map[string]Participant)

	for k, v := range m {
		copiedMap[k] = v
	}

	return copiedMap
}

func (d *Draw) availableParticipants(prizeNo int) []Participant {
	participants := copyParticipantMap(d.participants)

	// Remove winners
	for _, winners := range d.winners {
		for _, winner := range winners {
			delete(participants, winner.ID)
		}
	}

	return participantMapToSlice(participants)
}

func (d *Draw) AvailableParticipants(prizeNo int) []Participant {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	return d.availableParticipants(prizeNo)
}

func (d *Draw) Winners(prizeNo int) []Participant {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if _, ok := d.winners[prizeNo]; !ok {
		return []Participant{}
	}

	return d.winners[prizeNo]
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

func (d *Draw) Draw(prizeNo int) ([]Participant, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	winners := []Participant{}

	if _, ok := d.prizes[prizeNo]; !ok {
		return winners, ErrPrizeNo
	}

	amount := d.prizes[prizeNo].Amount
	if amount < 1 {
		return winners, ErrPrizeAmount
	}

	if _, ok := d.winners[prizeNo]; ok {
		return winners, ErrWinnersExistBeforeDraw
	}

	participants := d.availableParticipants(prizeNo)
	if len(participants) == 0 {
		return winners, ErrNoAvailableParticipants
	}

	winners = draw(amount, participants)

	d.winners[prizeNo] = winners
	return winners, nil
}

// Revoke revokes the winners of the given prize.
// It'll remove revoked winners from winners of the prize.
func (d *Draw) Revoke(prizeNo int, revokedWinners []Participant) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if _, ok := d.prizes[prizeNo]; !ok {
		return ErrPrizeNo
	}

	amount := d.prizes[prizeNo].Amount
	if amount < 1 {
		return ErrPrizeAmount
	}

	if _, ok := d.winners[prizeNo]; !ok {
		return ErrNoOriginalWinnersBeforeRedraw
	}

	// Remove original winners for the prize before re-draw.
	originalWinnerMap := participantSliceToMap(d.winners[prizeNo])

	for _, revokedWinner := range revokedWinners {
		if _, ok := originalWinnerMap[revokedWinner.ID]; !ok {
			return ErrRevokedWinnerNotMatch
		}
		delete(originalWinnerMap, revokedWinner.ID)
	}

	d.winners[prizeNo] = participantMapToSlice(originalWinnerMap)
	return nil
}

func (d *Draw) Redraw(prizeNo int, amount int) ([]Participant, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	winners := []Participant{}

	if _, ok := d.prizes[prizeNo]; !ok {
		return winners, ErrPrizeNo
	}

	if d.prizes[prizeNo].Amount < 1 {
		return winners, ErrPrizeAmount
	}

	if _, ok := d.winners[prizeNo]; !ok {
		return winners, ErrWinnersNotExistBeforeReDraw
	}

	if amount > d.prizes[prizeNo].Amount-len(d.winners[prizeNo]) {
		return winners, ErrRedrawPrizeAmount
	}

	participants := d.availableParticipants(prizeNo)
	if len(participants) == 0 {
		return winners, ErrNoAvailableParticipants
	}

	// Get new winners.
	winners = draw(amount, participants)

	// Append new winners and original winners.
	d.winners[prizeNo] = append(d.winners[prizeNo], winners...)
	return winners, nil
}

func (d *Draw) AllWinners() map[int][]Participant {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	return d.winners
}

func (d *Draw) ClearWinners(prizeNo int) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	// Clear the winner slice.
	d.winners[prizeNo] = []Participant{}
}

func (d *Draw) ClearAllWinners() {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	d.winners = make(map[int][]Participant)
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

func (d *Draw) Save(w io.Writer) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	tm := time.Now()

	data := SaveData{
		d.name,
		d.prizes,
		d.participants,
		d.winners,
		fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d",
			tm.Year(),
			tm.Month(),
			tm.Day(),
			tm.Hour(),
			tm.Minute(),
			tm.Second(),
		),
		fmt.Sprintf("%X", computeWinnersHash(d.winners)),
	}

	enc := json.NewEncoder(w)
	enc.SetIndent("", "    ")
	return enc.Encode(&data)
}

func (d *Draw) SaveToFile() error {
	dataFile := makeDataFileName(d.name)

	f, err := os.Create(dataFile)
	if err != nil {
		return err
	}
	defer f.Close()

	return d.Save(f)
}

func (d *Draw) Load(r io.Reader) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	data := SaveData{}
	dec := json.NewDecoder(r)

	if err := dec.Decode(&data); err != nil {
		return err
	}

	checksum := computeWinnersHash(data.Winners)
	if fmt.Sprintf("%X", checksum) != data.Checksum {
		return ErrChecksum
	}

	d.prizes = data.Prizes
	d.participants = data.Participants
	d.winners = data.Winners

	// Check if map is nil
	if d.prizes == nil {
		d.prizes = make(map[int]Prize)
	}

	if d.participants == nil {
		d.participants = make(map[string]Participant)
	}

	if d.winners == nil {
		d.winners = make(map[int][]Participant)
	}

	return nil
}

func (d *Draw) LoadFromFile() error {
	dataFile := makeDataFileName(d.name)

	f, err := os.Open(dataFile)
	if err != nil {
		return err
	}
	defer f.Close()

	return d.Load(f)
}

func (d *Draw) DataFileExists() bool {
	dataFile := makeDataFileName(d.name)

	if _, err := os.Stat(dataFile); os.IsNotExist(err) {
		return false
	}
	return true
}
