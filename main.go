package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/robfig/cron/v3"
)

var (
	rdb *redis.Client
	ctx = context.Background()
)

const responsesHashKey = "responses"

type EnqueuedRequest struct {
	ID      string                 `json:"id"`
	Request map[string]interface{} `json:"request"`
}

func main() {
	// Inicializar cliente Redis
	rdb = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	// Inicializar Gin
	router := gin.Default()

	// Ruta para recibir peticiones
	router.POST("/enqueue", func(c *gin.Context) {
		var request map[string]interface{}
		if err := c.BindJSON(&request); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		// Intentar acceder al objeto "Detalle" y luego al "CodigoGeneracion" dentro de él
		detalle, ok := request["detalle"].(map[string]interface{})
		if !ok || detalle == nil {
			// Imprimir el error en consola
			fmt.Println("detalle missing or invalid")
			fmt.Println(request)
			c.JSON(http.StatusBadRequest, gin.H{"error": "detalle missing or invalid"})
			return
		}

		id, ok := detalle["codigoGeneracion"].(string)
		if !ok || id == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "CodigoGeneracion missing or invalid"})
			fmt.Println("CodigoGeneracion missing or invalid")
			return
		}
		exists, err := rdb.SIsMember(ctx, "codigosGeneracion", id).Result()

		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to verify CodigoGeneracion"})
			return
		}
		if exists {
			c.JSON(http.StatusOK, gin.H{"message": "Ya existe una invalidación encolada para este DTE", "CodigoGeneracion": id})
			return
		}
		// Crear la estructura EnqueuedRequest
		enqueuedRequest := EnqueuedRequest{
			ID:      id,
			Request: request,
		}

		// Serializar la estructura EnqueuedRequest a JSON para encolar
		requestBytes, err := json.Marshal(enqueuedRequest)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to serialize enqueued request"})
			return
		}

		// Encolar la petición serializada en Redis
		if err := rdb.LPush(ctx, "requestsQueue", requestBytes).Err(); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to enqueue request"})
			return
		}

		c.JSON(http.StatusOK, gin.H{"message": "Invalidacion encolada en contingencia", "id": id})
	})

	router.GET("/status/:id", func(c *gin.Context) {
		id := c.Param("id")

		processed, response, err := getResponseStatus(id)
		if err != nil {
			// Si hay un error, envía una respuesta JSON con el error y el estado de procesamiento
			c.JSON(http.StatusInternalServerError, gin.H{
				"processed": processed,
				"error":     err.Error(),
			})
			return
		}

		if processed {
			var jsonResponse map[string]interface{}
			err := json.Unmarshal(response, &jsonResponse) // Intenta deserializar la respuesta JSON
			if err != nil {
				// Si hay un error al deserializar, envía la respuesta como un string crudo dentro de JSON
				c.JSON(http.StatusOK, gin.H{
					"processed": true,
					"response":  string(response),
				})
			} else {
				// Si la deserialización es exitosa, envía la respuesta como JSON
				c.JSON(http.StatusOK, gin.H{
					"processed": true,
					"response":  jsonResponse,
				})
			}
		} else {
			// Si la petición no ha sido procesada, informa que está pendiente
			c.JSON(http.StatusOK, gin.H{"processed": false, "response": "No procesado aún"})
		}
	})

	c := cron.New()
	lockKey := "processLock"
	lockValue := uuid.New().String()
	lockExpiration := time.Minute * 30

	_, err := c.AddFunc("@every 1m", func() {
		// Intentar adquirir el bloqueo
		if tryLock(lockKey, lockValue, lockExpiration) {
			defer releaseLock(lockKey, lockValue) // Asegurar que el bloqueo se libere al finalizar

			if isAPIAvailable() {
				fmt.Println("API disponible. Procesando peticiones encoladas...")
				processEnqueuedRequests() // Asume que esta función procesa y envía las peticiones encoladas
			} else {
				fmt.Println("API no disponible. Intentará nuevamente en 5 minutos.")
			}
		} else {
			fmt.Println("Otro proceso ya está en ejecución. Saliendo...")
		}
	})
	if err != nil {
		log.Fatalf("Error al programar la tarea: %v", err)
	}
	c.Start()

	// if isAPIAvailable() {
	// 	fmt.Println("API disponible. Procesando peticiones encoladas...")
	// 	processEnqueuedRequests() // Asume que esta función procesa y envía las peticiones encoladas
	// } else {
	// 	fmt.Println("API no disponible. Intentará nuevamente en 5 minutos.")
	// }

	router.Run(":5050")

}

func tryLock(key string, value string, expiration time.Duration) bool {
	result, err := rdb.SetNX(ctx, key, value, expiration).Result()
	if err != nil {
		log.Printf("Error al intentar establecer el bloqueo: %v", err)
		return false
	}
	return result
}

func releaseLock(key string, value string) {
	// Verificar que el valor coincida para asegurar que solo el que adquirió el bloqueo pueda liberarlo
	currentValue, err := rdb.Get(ctx, key).Result()
	if err != nil {
		log.Printf("Error al liberar el bloqueo: %v", err)
		return
	}
	if currentValue == value {
		_, err = rdb.Del(ctx, key).Result()
		if err != nil {
			log.Printf("Error al eliminar la clave de bloqueo: %v", err)
		}
	}
}

func checkAPIAndProcessRequests() {
	// Verificar disponibilidad de la API externa
	if isAPIAvailable() {
		fmt.Println("API externa disponible, procesando peticiones encoladas...")
		processEnqueuedRequests()
	} else {
		fmt.Println("API externa no disponible, reintentar más tarde...")
	}
}

func isAPIAvailable() bool {
	url := "https://apitest.dtes.mh.gob.sv/" // Asume que este es un endpoint de salud
	resp, err := http.Get(url)
	if err != nil {
		log.Printf("Error al verificar la disponibilidad de la API: %v", err)
		return false
	}
	defer resp.Body.Close()

	return resp.StatusCode == http.StatusServiceUnavailable
}

func processEnqueuedRequests() {
	token, err := authenticateAndGetToken()
	if err != nil {
		log.Printf("Error al obtener el token: %v", err) // Cambiado a log.Printf para no detener la ejecución
		return
	}

	for {
		result, err := rdb.RPop(ctx, "requestsQueue").Result()
		if err == redis.Nil {
			fmt.Println("No hay más peticiones en cola.")
			break
		} else if err != nil {
			log.Printf("Error al desencolar: %v", err) // Cambiado a log.Printf para continuar el bucle
			continue
		}

		var enqueuedRequest EnqueuedRequest
		if err := json.Unmarshal([]byte(result), &enqueuedRequest); err != nil {
			log.Printf("Error al deserializar la petición: %v", err)
			continue
		}

		// Verificar si la petición ya ha sido procesada revisando directamente en el hash de respuestas
		if isRequestProcessed(enqueuedRequest.ID) {
			fmt.Printf("La petición %s ya ha sido procesada.\n", enqueuedRequest.ID)
			continue
		}

		// Procesar la petición aquí...
		response, err := sendEnqueuedRequest(enqueuedRequest.Request, token)
		if err != nil {
			log.Printf("Error al enviar la petición encolada: %v", err)
			continue
		}

		//verificar la respuesta si se proceso o no exitosamente y no cambiarle el estatus
		// Analizar la respuesta para determinar si se procesó correctamente
		var responseObj map[string]interface{}
		if err := json.Unmarshal(response, &responseObj); err != nil {
			log.Printf("Error al deserializar la respuesta de la API: %v", err)
			continue
		}

		// Verificar si la respuesta contiene alguno de los mensajes específicos de error
		for _, key := range []string{"message", "Message"} {
			if message, ok := responseObj[key].(string); ok {
				// Comprueba si el mensaje contiene alguna de las frases clave
				if strings.Contains(message, "Ya existe una invalidación encolada para este DTE") ||
					strings.Contains(message, "Invalidacion encolada en contingencia") {
					// Nota: Se ha quitado el código específico del mensaje de contingencia para hacer la comprobación más general
					fmt.Printf("La petición %s no se marcó como procesada debido a: %s\n", enqueuedRequest.ID, message)
					continue
				}
			}
		}

		err = rdb.HSet(ctx, responsesHashKey, enqueuedRequest.ID, response).Err()
		if err != nil {
			log.Printf("Error al guardar la respuesta en Redis: %v", err)
		} else {
			fmt.Printf("Respuesta para la petición %s procesada y guardada en Redis con éxito.\n", enqueuedRequest.ID)
		}
	}
}

func authenticateAndGetToken() (string, error) {
	var token string
	var err error
	for i := 0; i < 3; i++ { // Intenta hasta 3 veces
		token, err = tryAuthenticate()
		if err == nil {
			return token, nil // Éxito, retorna el token
		}
		time.Sleep(time.Second * 2) // Espera 2 segundos antes de reintentar
		log.Printf("Reintento %d de autenticación fallida: %v", i+1, err)
	}
	return "", fmt.Errorf("autenticación fallida después de reintentos: %v", err)
}
func tryAuthenticate() (string, error) {
	// URL del endpoint de autenticación de la API
	url := "https://test.factured.sv/ApiFel/api/v1/signin/"

	// Cuerpo de la petición con las credenciales de autenticación
	requestBody, _ := json.Marshal(map[string]string{
		"User":     "fredfaradmin",
		"Password": "@jc1996#",
	})

	// Realizar la petición POST para autenticarse
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(requestBody))
	if err != nil {
		return "", err // Retorna el error para manejar reintentos
	}
	defer resp.Body.Close()

	// Leer y deserializar el cuerpo de la respuesta
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return "", err
	}

	// Extraer el token de autenticación del cuerpo de la respuesta
	authToken, ok := result["AuthToken"].(string)
	if !ok {
		return "", fmt.Errorf("AuthToken not found or invalid")
	}

	return authToken, nil // Retorna el token de autenticación obtenido
}

func sendEnqueuedRequest(request map[string]interface{}, token string) ([]byte, error) {
	url := "https://test.factured.sv/ApiFel/api/v1/dte/cancel"
	requestBody, _ := json.Marshal(request)

	client := &http.Client{}
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(requestBody))
	if err != nil {
		return nil, err
	}

	req.Header.Add("Authorization", "Bearer "+token)
	req.Header.Add("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	response, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func isRequestProcessed(id string) bool {
	exists, err := rdb.HExists(ctx, responsesHashKey, id).Result()
	if err != nil {
		log.Printf("Error al verificar si la petición está procesada: %v", err)
		return false
	}
	return exists
}

func markRequestAsProcessed(id string) {
	err := rdb.SAdd(ctx, "processedRequests", id).Err()
	if err != nil {
		log.Printf("Error al marcar la petición como procesada: %v", err)
	}
}

func getResponseStatus(id string) (bool, []byte, error) {
	processed := isRequestProcessed(id)

	if processed {
		// Si la petición fue procesada, intenta recuperar la respuesta del hash de respuestas
		response, err := rdb.HGet(ctx, responsesHashKey, id).Bytes()
		if err == redis.Nil {
			// La petición fue marcada como procesada, pero no hay respuesta almacenada en el hash.
			// Esto podría indicar un problema o que la respuesta fue eliminada.
			return true, nil, fmt.Errorf("la petición fue procesada pero no se encontró una respuesta")
		} else if err != nil {
			// Hubo un error al intentar recuperar la respuesta del hash
			return true, nil, err
		}
		// Respuesta encontrada y recuperada exitosamente del hash
		return true, response, nil
	}

	// La petición no ha sido procesada aún o no se encontró en el conjunto de procesadas
	return false, nil, nil
}
