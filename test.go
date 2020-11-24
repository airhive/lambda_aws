//https://www.alexedwards.net/blog/serverless-api-with-go-and-aws-lambda
package main

import (
        "fmt"
        "encoding/json"
        "context"
        "net/http"
        "github.com/aws/aws-lambda-go/lambda"
        "github.com/aws/aws-lambda-go/events"
)

// Response risposta
type Response struct {
        StatusCode  int       `json:"statusCode"`
        Headers     map[string]string  `json:"headers"`
        Body        string    `json:"body"`
        isBase64Encoded bool
}

// Testo il body
type Testo struct {
        Hello  string
}

// MyEvent per le cose che arrivano
type MyEvent struct {
        Name string `json:"name"`
}


// HandleRequest il return
func HandleRequest(ctx context.Context, req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
        nome := new(MyEvent)
        err := json.Unmarshal([]byte(req.Body), nome)
        testo := &Testo{
                Hello: fmt.Sprintf("Dr %s", nome.Name),
              };
        body, err := json.Marshal(testo)
        return events.APIGatewayProxyResponse{
                StatusCode: http.StatusOK,
                Body:       string(body),
            }, err
}

func main() {
        lambda.Start(HandleRequest)
}