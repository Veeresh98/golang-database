package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"github.com/jcelliott/lumber"
	log "github.com/sirupsen/logrus"
)

const Version = "0.0.1"

type (
	Logger interface {
		Fatal(string, ...interface{})
		Error(string, ...interface{})
		Warning(string, ...interface{})
		Info(string, ...interface{})
		Debug(string, ...interface{})
		Trace(string, ...interface{})
	}

	Driver struct {
		mutex   sync.Mutex
		mutexes map[string]*sync.Mutex
		dir     string
		log     Logger
	}
)

type Options struct {
	Logger
}

func Stat(path string) (fi os.FileInfo, err error) {
	if fi, err = os.Stat(path); os.IsNotExist(err) {
		fi, err = os.Stat(path + ".json")
	}
	return
}

//to create a directory in a data base
//The filepath.Clean() function in Go language used to return the shortest path name equivalent to the specified path by purely lexical processing
//filepath when working with files
func New(dir string, options *Options) (*Driver, error) {
	dir = filepath.Clean(dir)
	opts := Options{}

	if options != nil {
		opts = *options
	}

	if opts.Logger == nil {
		opts.Logger = lumber.NewConsoleLogger((lumber.INFO))
	}

	driver := Driver{
		dir:     dir,
		mutexes: make(map[string]*sync.Mutex),
		log:     opts.Logger,
	}

	if _, err := os.Stat(dir); err == nil { // stats() a built-in function used to get the file status for a given file or directory.
		opts.Logger.Debug("Hey '%s' the data base is laready existed\n", dir)
		return &driver, nil
	}

	opts.Logger.Debug("creating the database at '%s' ...\n", dir)
	return &driver, os.MkdirAll(dir, 0755) //0755 is the permissiion for the os.filepath

}

func (d *Driver) Write(collection, resource string, v interface{}) error { //db.Write("users", value.Name, user
	if collection == "" {
		return fmt.Errorf("Missing collection - no place save the record")
	}

	if resource == "" {
		return fmt.Errorf("Missing resource - unable to save teh record oops! ( no name found) ")
	}

	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	dir := filepath.Join(d.dir, collection)
	finalepath := filepath.Join(dir, resource+".json")
	tmpath := finalepath + ".tmp"

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil
	}
	b, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		return nil
	}
	b = append(b, byte('\n'))

	if err := ioutil.WriteFile(tmpath, b, 0644); err != nil {
		return err
	}
	return os.Rename(tmpath, finalepath)

}

func (d *Driver) Read(collection, resource string, v interface{}) error {

	if collection == "" {
		return fmt.Errorf("Missing collection - unable to read!")
	}

	if resource == "" {
		return fmt.Errorf("Missing resource - unable to read record (no name)")
	}
	record := filepath.Join(d.dir, collection, resource)

	if _, err := Stat(record); err != nil {
		return err
	}
	b, err := ioutil.ReadFile(record + ".json")
	if err != nil {
		return nil
	}
	return json.Unmarshal(b, &v)

}

func (d *Driver) ReadAll(collection string) ([]string, error) {
	if collection == "" {
		return nil, fmt.Errorf("Missing collection - unable to read teh data ")
	}
	dir := filepath.Join(d.dir, collection)
	if _, err := Stat(dir); err != nil {
		return nil, err
	}

	files, _ := ioutil.ReadDir(dir) //ioutil going to read from every dir with.ReadDir

	var records []string

	for _, file := range files {
		b, err := ioutil.ReadFile(filepath.Join(dir, file.Name())) //reading the file with help of dir by ioutill

		if err != nil {
			return nil, err
		}
		records = append(records, string(b))
	}

	return records, nil // everything goes smooth goings to return records and nill
}

func (d *Driver) Delete(collection, resource string) error {

	path := filepath.Join(collection, resource)

	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	dir := filepath.Join(d.dir, path)

	//let's check wether file existed or not

	switch fi, err := Stat(dir); {
	case fi == nil, err != nil:
		return fmt.Errorf("Unable to find the directory, Oops! %v\n", path)

	case fi.Mode().IsDir(): // going to delete the whole directory
		return os.RemoveAll(dir)

	case fi.Mode().IsRegular(): // going to delete all the file
		return os.RemoveAll(dir + ".josn")
	}

	return nil

}

func (d *Driver) getOrCreateMutex(collection string) *sync.Mutex {

	d.mutex.Lock()
	defer d.mutex.Unlock()
	key, value := d.mutexes[collection]

	if !value {
		key = &sync.Mutex{}
		d.mutexes[collection] = key
	}
	return key
}

type user struct {
	Name    string
	Age     json.Number
	Contact string
	Company string
	Address Address
}

type Address struct {
	City    string
	State   string
	Pincode json.Number
	Country string
}

func main() {

	//creating a directory
	dir := "./"
	db, err := New(dir, nil)

	// checking the error
	if err != nil {
		fmt.Println("Error", err)
	}
	//creating few eomployee details
	employee := []user{
		{"veeresh", "22", "3693653", "capgemini", Address{"Bangalore", "Karnataka", "560056", "India"}},
		{"Manu", "24", "33593653", "Mindtree", Address{"Mangalore", "Karnataka", "561056", "India"}},
		{"Pratiksha", "21", "345693653", "capgemini", Address{"Pune", "Maharastra", "410056", "India"}},
		{"Bhavana", "23", "369362436", "IBM", Address{"Bangalore", "Karnataka", "560056", "India"}},
		{"Shashank", "24", "36924653", "Infosys", Address{"Udupi", "Karnataka", "564056", "India"}},
		{"Deepika", "26", "36356653", "IBM", Address{"Bangalore", "Karnataka", "560056", "India"}},
		{"Chitra", "26", "3764653", "Vodophone", Address{"Pune", "Maharastra", "410056", "India"}},
	}

	//ranging over the eomployee and using Write function to crete a db function to create
	for _, value := range employee {
		db.Write("users", value.Name, user{ //created a datgabase function as write and creatinf eomplyee details(users is a collection)
			Name:    value.Name,
			Age:     value.Age,
			Contact: value.Contact,
			Company: value.Company,
			Address: value.Address,
		})

		fmt.Println(value)
	}

	//after  creating reading the details

	records, err := db.ReadAll("users")
	log.Infof("get the records through loggers", records)
	if err != nil {
		fmt.Println("Error", err)
	}
	fmt.Println("see the recorrds", records)

	allusers := []user{}
	for _, item := range records {
		employeefound := user{}
		if err := json.Unmarshal([]byte(item), &employeefound); err != nil { //unmarshal is used convert byte data into original data structure
			fmt.Println("Error", err)
		}
		allusers = append(allusers, employeefound)
		log.Infof("get the allusers after reading from the records", allusers)

	}
	//gping to call this through the api
	// if err := db.Delete("users", "Manu"); err != nil {  // to delete a particular name
	// 	fmt.Println("Error", err)
	// }

	// if err := db.Delete("users",""); err != nil{
	// 	fmt.Println("Error",err)
	// }
}
