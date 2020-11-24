package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	_ "github.com/go-sql-driver/mysql"
	"github.com/graphql-go/graphql"
)

// Response risposta
type Response struct {
	StatusCode      int               `json:"statusCode"`
	Headers         map[string]string `json:"headers"`
	Body            string            `json:"body"`
	isBase64Encoded bool
}

// MyEvent per le cose che arrivano
type MyEvent struct {
	Query  string `json:"query"`
	Loc    string `json:"loc"`
	Giorni int    `json:"giorni"`
}

// Il database viene aperto solo una volta.
var db *sql.DB

// Features contiene tutto
type Features struct {
	Type       string
	Geometry   Geometry
	Properties Sensori
}

// Geometry contiene Type e Coordinates
type Geometry struct {
	Type        string
	Coordinates [2]float32
}

// Sensori contiene i dati dei sensori
type Sensori struct {
	IDSensore string
	Pm10      float32
	Pm10p1    float32
	Pm10p2    float32
	Pm10p3    float32
	Pm10p4    float32
	Temp      float32
	Umi       float32
	Prec      float32
	Vento     float32
	No2       float32
	No2p1     float32
	No2p2     float32
	No2p3     float32
	No2p4     float32
	O3        float32
	O3p1      float32
	O3p2      float32
	O3p3      float32
	O3p4      float32
}

// Lanciato quando carico su lambda
func init() {
	// Apre connessione al db modificando la globale
	var err error
	db, err = sql.Open("mysql", "PASS")
	if err != nil {
		log.Fatalf("Errore nell'aprire la connessione col database: %v", err)
		return
	}

	db.SetMaxIdleConns(20)
	db.SetMaxOpenConns(0)
	db.SetConnMaxLifetime(time.Minute * 20)

	err = db.Ping()
	if err != nil {
		log.Fatalf("Errore nel mantenere la connessione col database: %v", err)
		return
	}
}

func risolviSchema(query string, loc []string, quantiGiorniFa int) ([]byte, error) {
	var sensoriType = graphql.NewObject(
		graphql.ObjectConfig{
			Name: "Sensori",
			Fields: graphql.Fields{
				"idsensore": &graphql.Field{
					Type: graphql.String,
				},
				"pm10": &graphql.Field{
					Type: graphql.Float,
				},
				"pm10p1": &graphql.Field{
					Type: graphql.Float,
				},
				"pm10p2": &graphql.Field{
					Type: graphql.Float,
				},
				"pm10p3": &graphql.Field{
					Type: graphql.Float,
				},
				"pm10p4": &graphql.Field{
					Type: graphql.Float,
				},
				"temp": &graphql.Field{
					Type: graphql.Float,
				},
				"umi": &graphql.Field{
					Type: graphql.Float,
				},
				"prec": &graphql.Field{
					Type: graphql.Float,
				},
				"vento": &graphql.Field{
					Type: graphql.Float,
				},
				"no2": &graphql.Field{
					Type: graphql.Float,
				},
				"no2p1": &graphql.Field{
					Type: graphql.Float,
				},
				"no2p2": &graphql.Field{
					Type: graphql.Float,
				},
				"nop3": &graphql.Field{
					Type: graphql.Float,
				},
				"no2p4": &graphql.Field{
					Type: graphql.Float,
				},
				"o3": &graphql.Field{
					Type: graphql.Float,
				},
				"o3p1": &graphql.Field{
					Type: graphql.Float,
				},
				"o3p2": &graphql.Field{
					Type: graphql.Float,
				},
				"o3p3": &graphql.Field{
					Type: graphql.Float,
				},
				"o3p4": &graphql.Field{
					Type: graphql.Float,
				},
			},
		},
	)

	var geometryType = graphql.NewObject(
		graphql.ObjectConfig{
			Name: "Geometry",
			Fields: graphql.Fields{
				"type": &graphql.Field{
					Type: graphql.String,
				},
				"coordinates": &graphql.Field{
					Type: graphql.NewList(graphql.Float),
				},
			},
		},
	)

	var featuresType = graphql.NewObject(
		graphql.ObjectConfig{
			Name: "Features",
			Fields: graphql.Fields{
				"type": &graphql.Field{
					Type: graphql.String,
				},
				"geometry": &graphql.Field{
					Type: geometryType,
				},
				"properties": &graphql.Field{
					Type: sensoriType,
				},
			},
		},
	)

	var numero int
	numeroSensori, err := db.Query(fmt.Sprintf("SELECT COUNT(*) FROM id_sensori%v", loc[0]))
	if err != nil {
		log.Printf("%v", err)
	}
	for numeroSensori.Next() {
		numeroSensori.Scan(&numero)
	}
	numeroSensori.Close()
	ultimoValore := numero * quantiGiorniFa

	// Schema
	fields := graphql.Fields{
		"type": &graphql.Field{
			Type: graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return "FeatureCollection", nil
			},
		},
		"tempo": &graphql.Field{
			Type:        graphql.String,
			Description: "Tempo rilevazione",
			Resolve: func(params graphql.ResolveParams) (interface{}, error) {
				var tempo string
				tempoDaDatabase, err := db.Query(fmt.Sprintf("SELECT tempo FROM sensori ORDER BY tempo DESC, id_sensore ASC LIMIT %d", ultimoValore-quantiGiorniFa))
				if err != nil {
					log.Printf("Errore nel tempo, prendo l'ultimo: %v", err)
					tempoDaDatabase, err = db.Query(fmt.Sprintf("SELECT tempo FROM sensori ORDER BY tempo DESC, id_sensore ASC LIMIT 1"))
					if err != nil {
						log.Printf("Errore nel tempo prendendo l'ultimo: %v", err)
					}
				}
				defer tempoDaDatabase.Close()
				for tempoDaDatabase.Next() {
					err = tempoDaDatabase.Scan(&tempo)
					if err != nil {
						log.Printf("%v", err)
					}
				}
				return tempo, nil
			},
		},
		"features": &graphql.Field{
			Type:        graphql.NewList(featuresType),
			Description: "Ottieni i dati dai sensori",
			Resolve: func(params graphql.ResolveParams) (interface{}, error) {
				var features []Features
				for _, location := range loc {
					idSensori, err := db.Query(fmt.Sprintf("SELECT * FROM id_sensori%v", location))
					if err != nil {
						log.Printf("%v", err)
						idSensori.Close()
						return nil, fmt.Errorf("Questo posto non è nel database")
					}
					previsioni, err := db.Query("SELECT * FROM previsioni")
					if err != nil {
						log.Printf("%v", err)
					}
					var results *sql.Rows
					if location != "meteoit" {
						results, err = db.Query(fmt.Sprintf("SELECT pm10, temp, umi, prec, vento, no2, o3 FROM sensori ORDER BY tempo DESC, id_sensore ASC LIMIT %d, %d", ultimoValore-numero, ultimoValore))
						if err != nil {
							log.Printf("%v", err)
						}
					} else {
						fmt.Printf("SELECT pm10, pm25, umi, prec, vento, no, o3 FROM meteoit ORDER BY id DESC LIMIT %d", numero)
						results, err = db.Query(fmt.Sprintf("SELECT pm10, pm25, umi, prec, vento, no, o3 FROM meteoit ORDER BY id DESC LIMIT %d", numero))
						if err != nil {
							return nil, fmt.Errorf("Errore nel leggere quel database: %v", err)
						}
					}
					for idSensori.Next() {
						previsioni.Next()
						results.Next()
						var feature Features
						feature.Type = "Feature"
						feature.Geometry.Type = "Point"
						err = previsioni.Scan(&feature.Properties.Pm10p1, &feature.Properties.Pm10p2, &feature.Properties.Pm10p3, &feature.Properties.Pm10p4, &feature.Properties.No2p1, &feature.Properties.No2p2, &feature.Properties.No2p3, &feature.Properties.No2p4, &feature.Properties.O3p1, &feature.Properties.O3p2, &feature.Properties.O3p3, &feature.Properties.O3p4)
						if err != nil {
							log.Printf("%v", err)
						}
						err = idSensori.Scan(&feature.Properties.IDSensore, &feature.Geometry.Coordinates[1], &feature.Geometry.Coordinates[0])
						if err != nil {
							log.Printf("%v", err)
						}
						err = results.Scan(&feature.Properties.Pm10, &feature.Properties.Temp, &feature.Properties.Umi, &feature.Properties.Prec, &feature.Properties.Vento, &feature.Properties.No2, &feature.Properties.O3)
						if err != nil {
							log.Printf("%v", err)
						}
						features = append(features, feature)
					}
					idSensori.Close()
					previsioni.Close()
					results.Close()
				}
				return features, nil
			},
		},
	}
	rootQuery := graphql.ObjectConfig{Name: "RootQuery", Fields: fields}
	schemaConfig := graphql.SchemaConfig{Query: graphql.NewObject(rootQuery)}
	schema, err := graphql.NewSchema(schemaConfig)
	if err != nil {
		return nil, fmt.Errorf("Errore nella creazione dello schema, errore: %v", err)
	}

	params := graphql.Params{Schema: schema, RequestString: query}
	r := graphql.Do(params)
	if len(r.Errors) > 0 {
		return nil, fmt.Errorf("Errore nell'esecuzione della query graphql, errore: %+v", r.Errors)
	}
	jsonData, err := json.Marshal(r)
	if err != nil {
		return nil, fmt.Errorf("Errore nella conversione in JSON, errore: %v", err)
	}

	return jsonData, nil
}

func getJSON(parametri *MyEvent) string {
	// Prendi la query se c'è
	query := parametri.Query
	if query == "" {
		query = `
		{
			tempo
			type
			features{
				type
				geometry {
					type
					coordinates
				}
				properties {
					idsensore
					pm10
					temp
					prec
					vento
					no2
					o3
				}
			}
		}
		`
	}
	// Prendi le richieste della posizione
	locDate := parametri.Loc
	if locDate == "" {
		HandleErrori(http.StatusBadRequest, "Specifica una location con ?loc=")
	}
	loc := strings.Split(locDate, ",")
	if loc[0] == "veneto" {
		loc = []string{"vicenza"}
	}
	if loc[0] == "trentino" {
		loc = []string{"merano", "bolzano"}
	}
	if loc[0] == "lombardia" {
		loc = []string{"milano"}
	}
	if loc[0] == "tutto" {
		loc = []string{"milano", "merano", "bolzano", "vicenza", "meteoit"}
	}
	// Prendi i dati di quanti istanti fa?
	// Nella richiesta giorni=0 è l'ultimo istante campionato.
	tempo := parametri.Giorni
	var quantiGiorniFa int
	var err error
	if tempo == 0 {
		quantiGiorniFa = 1
	} else {
		// 0 diventa 1
		quantiGiorniFa = tempo + 1
	}
	if quantiGiorniFa < 1 {
		HandleErrori(http.StatusBadRequest, "Per vedere il futuro usare la query.")
	}
	jsonData, err := risolviSchema(query, loc, quantiGiorniFa)
	if err != nil {
		log.Printf(fmt.Sprintf("%v", err))
		HandleErrori(http.StatusBadRequest, fmt.Sprintf("%v", err))
	}
	return string(jsonData)
}

// HandleErrori ritorna in caso di errore
func HandleErrori(status int, testo string) (events.APIGatewayProxyResponse, error) {
	log.Println(testo)

	return events.APIGatewayProxyResponse{
		StatusCode: status,
		Body:       testo,
	}, nil
}

// HandleRequest il return
func HandleRequest(ctx context.Context, req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	//TODO: non vede l'header
	//if req.Headers["Content-Type"] != "application/json" {
	//    return HandleErrori(http.StatusBadRequest, "Header non specifica json.")
	//}

	parametri := new(MyEvent)
	err := json.Unmarshal([]byte(req.Body), parametri)

	if err != nil {
		log.Printf("Errore nell'unmarshal, err: %s", err)
		log.Printf(req.Body)
		return HandleErrori(http.StatusInternalServerError, http.StatusText(http.StatusInternalServerError))
	}

	return events.APIGatewayProxyResponse{
		Headers:    map[string]string{"Access-Control-Allow-Origin": "https://www.airhive.it"},
		StatusCode: http.StatusOK,
		Body:       getJSON(parametri),
	}, nil
}

func main() {
	lambda.Start(HandleRequest)
}
