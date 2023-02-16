package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"io/ioutil"
	"github.com/jcelliott/lumber"
)


type (
	Logger interface{
		Fatal(string,  ...any)
		Error(string,  ...any)
		Warn(string,  ...any)
		Info(string,  ...any)
		Debug(string,  ...any)
		Trace(string, ...any)	
		
	}

	Driver struct{
		mutex sync.Mutex
		mutexes map[string]*sync.Mutex
		dir string
		log Logger
	}	
)

type Options struct{
	Logger
}

func New(dir string, opts *Options)(*Driver, error){
	dir = filepath.Clean(dir)
	opt := Options{}
	if opts != nil{
		opt = *opts
	}
	if opt.Logger != nil{
		opt.Logger = lumber.NewConsoleLogger(lumber.INFO)
	}
	driver := &Driver{
		dir: dir,
		log: opt.Logger,
		mutexes : make(map[string]*sync.Mutex),
	}
	// Check if the directory exist
	if _, err := os.Stat(dir); os.IsNotExist(err){
		opt.Logger.Debug("Using '%s' (database already exist)\n", dir)
		return driver, nil
	}
	// opts.Logger.Debug("Creating the database at '%s'...\n", dir)
	return driver, os.MkdirAll(dir, 0755)

}

func stat(path string)(fi os.FileInfo, err error){
	if fi, err = os.Stat(path); os.IsNotExist(err){
		fi, err = os.Stat(path + ".json")
	}
	return
}

func (d *Driver) Write(collection, document string, v any) error{
	if collection == ""{
		return fmt.Errorf("missing collection to store document")
	}
	if document == ""{
		return fmt.Errorf("missing document - unable to save record")
	}
	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	dir := filepath.Join(d.dir, collection)
	fnlPath := filepath.Join(dir, document + ".json")
	tmpPath := fnlPath + ".tmp"
	if err := os.MkdirAll(dir, 0755); err != nil{
		return err
	}
	b, err := json.MarshalIndent(v, "", "\t")
	if err != nil{
		return err
	}
	b = append(b, byte('\n'))

	if err := ioutil.WriteFile(tmpPath, b, 0644); err != nil{
		return err
	}
	return os.Rename(tmpPath, fnlPath)
}

func (d *Driver) ReadAll(collection string) ([]string, error){
	if collection == ""{
		return nil, fmt.Errorf("missing collection to store document")
	}
	dir := filepath.Join(d.dir, collection)
	if _, err := stat(dir); err != nil{
		return nil, err
	}
	files, _ := ioutil.ReadDir(dir)
	var records []string
	for _, file := range files{
		b,err := ioutil.ReadFile(filepath.Join(dir, file.Name()))
		if err != nil{
			return nil, err
		}

		records = append(records, string(b))
	}
	return records, nil
}

func (d *Driver) Read(collection, document string, v any) error{
	if collection == ""{
		return fmt.Errorf("missing collection to store document")
	}
	if document == ""{
		return fmt.Errorf("missing document - unable to save record")
	}
	record := filepath.Join(d.dir, collection, document)
	if _, err := stat(record); err != nil{
		return err
	}
	b,err := ioutil.ReadFile(record + ".json")
	if err != nil{
		return err
	}
	return json.Unmarshal(b, &v)

}

func (d *Driver) Delete(collection, document string)error{
	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()
	path := filepath.Join(collection, document)
	dir := filepath.Join(d.dir, path)
	switch fi, err := stat(dir);{
	case fi == nil, err != nil:
			return fmt.Errorf("unable to find the file")
	case fi.Mode().IsDir():
		 return os.RemoveAll(dir)
	case fi.Mode().IsRegular():
		return os.Remove(dir + ".json")
	}
	return nil
}
func (d *Driver) DeleteAll(){}

func (d *Driver)getOrCreateMutex(collection string) *sync.Mutex{
	d.mutex.Lock()
	defer d.mutex.Unlock()
	m, ok := d.mutexes[collection]
	if !ok{
		m = &sync.Mutex{}
		d.mutexes[collection] = m
	}
	return m
}

type User struct {
	Name    string
	Age     json.Number
	Contact string
	Company string
	Address Address
}
type Address struct {
	City    string
	State   string
	Country string
	PinCode json.Number
}

func main() {
	dir := "./"

	db, err := New(dir, nil)
	if err != nil{
		log.Fatal(err)
	}
	employees := []User{
		{"Jason","22","08137450565", "Thyaza", Address{"Lagos", "Lagos","Nigeria", "110235"}},
		{"Peter","22","08137450565", "SocialCab", Address{"Lagos", "Lagos","Nigeria", "110235"}},
		{"Al","22","08137450565", "LifeChannels", Address{"Lagos", "Lagos","Nigeria", "110235"}},
		{"Emma","22","08137450565", "ByteFoods", Address{"Lagos", "Lagos","Nigeria", "110235"}},
		{"Faith","22","08137450565", "Helimentals", Address{"Lagos", "Lagos","Nigeria", "110235"}},
		{"John","22","08137450565", "AiSmith", Address{"Lagos", "Lagos","Nigeria", "110235"}},
	}
	for _, value := range employees{
		db.Write("unicorns", value.Name, User{
			Name: value.Name,
			Age: value.Age,
			Contact: value.Contact,
			Company: value.Company,
			Address: value.Address,
		})
	}
	records, err := db.ReadAll("unicorns")
	if err != nil{
		log.Fatal(err)
	}
	fmt.Println(records)

	allUsers := []User{}
	for _, rec := range records{
		employeesFound := User{}
		if err := json.Unmarshal([]byte(rec), &employeesFound); err != nil{
			log.Fatal(err)
		}
		allUsers = append(allUsers, employeesFound)
	}
	fmt.Println(allUsers)

}
