package mapping

import (
	"os"
	"strings"

	"github.com/port-labs/port-k8s-exporter/pkg/jq"
	"github.com/port-labs/port-k8s-exporter/pkg/port"
)

func replacePlaceholder(input string) string {
	clusterName := os.Getenv("CLUSTER_NAME")
	return strings.ReplaceAll(input, "{CLUSTER_NAME}", "\""+clusterName+"\"")
}

func replacePlaceholdersInMap(inputMap map[string]string) map[string]string {
	processedMap := make(map[string]string)
	for key, value := range inputMap {
		processedMap[key] = replacePlaceholder(value)
	}
	return processedMap
}

func NewEntity(obj interface{}, mapping port.EntityMapping) (*port.Entity, error) {
	var err error
	entity := &port.Entity{}

	// Process individual fields with replacePlaceholder
	entity.Identifier, err = jq.ParseString(replacePlaceholder(mapping.Identifier), obj)
	if err != nil {
		return &port.Entity{}, err
	}
	if mapping.Title != "" {
		entity.Title, err = jq.ParseString(replacePlaceholder(mapping.Title), obj)
		if err != nil {
			return &port.Entity{}, err
		}
	}
	entity.Blueprint, err = jq.ParseString(replacePlaceholder(mapping.Blueprint), obj)
	if err != nil {
		return &port.Entity{}, err
	}
	if mapping.Team != "" {
		entity.Team, err = jq.ParseInterface(mapping.Team, obj)
		if err != nil {
			return &port.Entity{}, err
		}
	}

	processedProperties, err := jq.ParseMapInterface(replacePlaceholdersInMap(mapping.Properties), obj)
	if err != nil {
		return &port.Entity{}, err
	}
	entity.Properties = processedProperties

	processedRelations, err := jq.ParseMapInterface(replacePlaceholdersInMap(mapping.Relations), obj)
	if err != nil {
		return &port.Entity{}, err
	}
	entity.Relations = processedRelations

	return entity, err

}
