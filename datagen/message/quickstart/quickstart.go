package quickstart

import (
	"spitha/datagen/datagen/message/protobuf"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/jinzhu/copier"
)

/**********************************************************************
**                                                                   **
**                   Person struct for quickstart                    **
**                                                                   **
***********************************************************************/
type PersonInfo struct {
	FirstName  string          `json:"first_name" xml:"first_name" avro:"first_name"`
	LastName   string          `json:"last_name" xml:"last_name" avro:"last_name"`
	Gender     string          `json:"gender" xml:"gender" avro:"gender"`
	SSN        string          `json:"ssn" xml:"ssn" avro:"ssn"`
	Hobby      string          `json:"hobby" xml:"hobby" avro:"hobby"`
	Job        *JobInfo        `json:"job" xml:"job" avro:"job"`
	Address    *AddressInfo    `json:"address" xml:"address" avro:"address"`
	Contact    *ContactInfo    `json:"contact" xml:"contact" avro:"contact"`
	CreditCard *CreditCardInfo `json:"credit_card" xml:"credit_card" avro:"credit_card"`
}

type CreditCardInfo struct {
	Type   string `json:"type" xml:"type" avro:"type"`
	Number string `json:"number" xml:"number" avro:"number"`
	Exp    string `json:"exp" xml:"exp" avro:"exp"`
	Cvv    string `json:"cvv" xml:"cvv" avro:"cvv"`
}

// make random person data
func MakeRandomPerson() PersonInfo {
	var person PersonInfo
	radomPerson := gofakeit.Person()
	copier.Copy(&person, &radomPerson)
	return person
}

func MakeRandomPersonForProtoBuf() *protobuf.PersonInfo {
	p := gofakeit.Person()

	return &protobuf.PersonInfo{
		FirstName: p.FirstName,
		LastName:  p.LastName,
		Gender:    p.Gender,
		Ssn:       gofakeit.SSN(),
		Hobby:     gofakeit.Hobby(),
		Job: &protobuf.PersonInfo_JobInfo{
			Company:     p.Job.Company,
			Title:       p.Job.Title,
			Descriptor_: p.Job.Descriptor,
			Level:       p.Job.Level,
		},
		Address: &protobuf.PersonInfo_AddressInfo{
			Address:   p.Address.Address,
			Street:    p.Address.Street,
			City:      p.Address.City,
			State:     p.Address.State,
			Zip:       p.Address.Zip,
			Country:   p.Address.Country,
			Latitude:  p.Address.Latitude,
			Longitude: p.Address.Longitude,
		},
		Contact: &protobuf.PersonInfo_ContactInfo{
			Phone: p.Contact.Phone,
			Email: p.Contact.Email,
		},
		CreditCard: &protobuf.PersonInfo_CreditCardInfo{
			Type:   p.CreditCard.Type,
			Number: p.CreditCard.Number,
			Exp:    p.CreditCard.Exp,
			Cvv:    p.CreditCard.Cvv,
		},
	}
}

/**********************************************************************
**                                                                   **
**                    Book struct for quickstart                     **
**                                                                   **
***********************************************************************/
type BookInfo struct {
	Title  string `json:"title" xml:"title" avro:"title"`
	Author string `json:"author" xml:"author" avro:"author"`
	Genre  string `json:"genre" xml:"genre" avro:"genre"`
}

// make random book data
func MakeRandomBook() BookInfo {
	var book BookInfo
	radomBook := gofakeit.Book()
	copier.Copy(&book, &radomBook)
	return book
}

func MakeRandomBookForProtoBuf() *protobuf.BookInfo {
	b := gofakeit.Book()
	return &protobuf.BookInfo{
		Title:  b.Title,
		Author: b.Author,
		Genre:  b.Genre,
	}
}

/**********************************************************************
**                                                                   **
**                     Car struct for quickstart                     **
**                                                                   **
***********************************************************************/
type CarInfo struct {
	Type         string `json:"type" xml:"type" avro:"type"`
	Fuel         string `json:"fuel" xml:"fuel" avro:"fuel"`
	Transmission string `json:"transmission" xml:"transmission" avro:"transmission"`
	Brand        string `json:"brand" xml:"brand" avro:"brand"`
	Model        string `json:"model" xml:"model" avro:"model"`
	Year         int    `json:"year" xml:"year" avro:"year"`
}

// make random car data
func MakeRandomCar() CarInfo {
	var car CarInfo
	radomCar := gofakeit.Car()
	copier.Copy(&car, &radomCar)
	return car
}

func MakeRandomCarForProtoBuf() *protobuf.CarInfo {
	c := gofakeit.Car()
	return &protobuf.CarInfo{
		Brand:        c.Brand,
		Fuel:         c.Fuel,
		Model:        c.Model,
		Transmission: c.Transmission,
		Type:         c.Type,
		Year:         int64(c.Year),
	}
}

/**********************************************************************
**                                                                   **
**                    Address struct for quickstart                  **
**                                                                   **
***********************************************************************/
type AddressInfo struct {
	Address   string  `json:"address" xml:"address" avro:"address"`
	Street    string  `json:"street" xml:"street" avro:"street"`
	City      string  `json:"city" xml:"city" avro:"city"`
	State     string  `json:"state" xml:"state" avro:"state"`
	Zip       string  `json:"zip" xml:"zip" avro:"zip"`
	Country   string  `json:"country" xml:"country" avro:"country"`
	Latitude  float64 `json:"latitude" xml:"latitude" avro:"latitude"`
	Longitude float64 `json:"longitude" xml:"longitude" avro:"longitude"`
}

// make random address data
func MakeRandomAddress() AddressInfo {
	var address AddressInfo
	radomAddress := gofakeit.Address()
	copier.Copy(&address, &radomAddress)
	return address
}

func MakeRandomAddressForProtoBuf() *protobuf.AddressInfo {
	a := gofakeit.Address()
	return &protobuf.AddressInfo{
		Address:   a.Address,
		Street:    a.Street,
		City:      a.City,
		State:     a.State,
		Zip:       a.Zip,
		Country:   a.Country,
		Latitude:  a.Latitude,
		Longitude: a.Longitude,
	}
}

/**********************************************************************
**                                                                   **
**                    Contact struct for quickstart                  **
**                                                                   **
***********************************************************************/
type ContactInfo struct {
	Phone string `json:"phone" xml:"phone" avro:"phone"`
	Email string `json:"email" xml:"email" avro:"email"`
}

// make random book data
func MakeRandomContact() ContactInfo {
	var contact ContactInfo
	radomContact := gofakeit.Contact()
	copier.Copy(&contact, &radomContact)
	return contact
}

func MakeRandomContactForProtoBuf() *protobuf.ContactInfo {
	c := gofakeit.Contact()
	return &protobuf.ContactInfo{
		Phone: c.Phone,
		Email: c.Email,
	}
}

/**********************************************************************
**                                                                   **
**                     Movie struct for quickstart                   **
**                                                                   **
***********************************************************************/
type MovieInfo struct {
	Name  string `json:"name" xml:"name" avro:"name"`
	Genre string `json:"genre" xml:"genre" avro:"genre"`
}

// make random job data
func MakeRandomMovie() MovieInfo {
	var movie MovieInfo
	radomMovie := gofakeit.Movie()
	copier.Copy(&movie, &radomMovie)
	return movie
}

func MakeRandomMovieForProtoBuf() *protobuf.MovieInfo {
	m := gofakeit.Movie()
	return &protobuf.MovieInfo{
		Name:  m.Name,
		Genre: m.Genre,
	}
}

/**********************************************************************
**                                                                   **
**                      Job struct for quickstart                    **
**                                                                   **
***********************************************************************/
type JobInfo struct {
	Company    string `json:"company" xml:"company" avro:"company"`
	Title      string `json:"title" xml:"title" avro:"title"`
	Descriptor string `json:"descriptor" xml:"descriptor" avro:"descriptor"`
	Level      string `json:"level" xml:"level" avro:"level"`
}

// make random job data
func MakeRandomJob() JobInfo {
	var job JobInfo
	radomJob := gofakeit.Job()
	copier.Copy(&job, &radomJob)
	return job
}

func MakeRandomJobForProtoBuf() *protobuf.JobInfo {
	j := gofakeit.Job()
	return &protobuf.JobInfo{
		Company:     j.Company,
		Title:       j.Title,
		Descriptor_: j.Descriptor,
		Level:       j.Level,
	}
}
