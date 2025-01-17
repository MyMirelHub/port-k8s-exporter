package handlers

import (
	"context"
	"github.com/port-labs/port-k8s-exporter/pkg/config"
	"github.com/port-labs/port-k8s-exporter/pkg/goutils"
	"github.com/port-labs/port-k8s-exporter/pkg/k8s"
	"github.com/port-labs/port-k8s-exporter/pkg/port/cli"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/klog/v2"
	"time"
)

type ControllersHandler struct {
	controllers      []*k8s.Controller
	informersFactory dynamicinformer.DynamicSharedInformerFactory
	exporterConfig   *config.Config
	portClient       *cli.PortClient
}

func NewControllersHandler(exporterConfig *config.Config, k8sClient *k8s.Client, portClient *cli.PortClient) *ControllersHandler {
	resync := time.Minute * time.Duration(exporterConfig.ResyncInterval)
	informersFactory := dynamicinformer.NewDynamicSharedInformerFactory(k8sClient.DynamicClient, resync)

	aggResources := make(map[string][]config.KindConfig)
	for _, resource := range exporterConfig.Resources {
		kindConfig := config.KindConfig{Selector: resource.Selector, Port: resource.Port}
		if _, ok := aggResources[resource.Kind]; ok {
			aggResources[resource.Kind] = append(aggResources[resource.Kind], kindConfig)
		} else {
			aggResources[resource.Kind] = []config.KindConfig{kindConfig}
		}
	}

	controllers := make([]*k8s.Controller, 0, len(exporterConfig.Resources))

	for kind, kindConfigs := range aggResources {
		var gvr schema.GroupVersionResource
		gvr, err := k8s.GetGVRFromResource(k8sClient.DiscoveryMapper, kind)
		if err != nil {
			klog.Errorf("Error getting GVR, skip handling for resource '%s': %s.", kind, err.Error())
			continue
		}

		informer := informersFactory.ForResource(gvr)
		controller := k8s.NewController(config.AggregatedResource{Kind: kind, KindConfigs: kindConfigs}, portClient, informer)
		controllers = append(controllers, controller)
	}

	if len(controllers) == 0 {
		klog.Fatalf("Failed to initiate a controller for all resources, exiting...")
	}

	controllersHandler := &ControllersHandler{
		controllers:      controllers,
		informersFactory: informersFactory,
		exporterConfig:   exporterConfig,
		portClient:       portClient,
	}

	return controllersHandler
}

func (c *ControllersHandler) Handle(stopCh <-chan struct{}) {
	klog.Info("Starting informers")
	c.informersFactory.Start(stopCh)
	klog.Info("Waiting for informers cache sync")
	for _, controller := range c.controllers {
		if err := controller.WaitForCacheSync(stopCh); err != nil {
			klog.Fatalf("Error while waiting for informer cache sync: %s", err.Error())
		}
	}
	klog.Info("Deleting stale entities")
	c.RunDeleteStaleEntities()
	klog.Info("Starting controllers")
	for _, controller := range c.controllers {
		controller.Run(1, stopCh)
	}

	<-stopCh
	klog.Info("Shutting down controllers")
	for _, controller := range c.controllers {
		controller.Shutdown()
	}
	klog.Info("Exporter exiting")
}

func (c *ControllersHandler) RunDeleteStaleEntities() {
	currentEntitiesSet := make([]map[string]interface{}, 0)
	for _, controller := range c.controllers {
		controllerEntitiesSet, err := controller.GetEntitiesSet()
		if err != nil {
			klog.Errorf("error getting controller entities set: %s", err.Error())
		}
		currentEntitiesSet = append(currentEntitiesSet, controllerEntitiesSet)
	}

	_, err := c.portClient.Authenticate(context.Background(), c.portClient.ClientID, c.portClient.ClientSecret)
	if err != nil {
		klog.Errorf("error authenticating with Port: %v", err)
	}

	err = c.portClient.DeleteStaleEntities(context.Background(), c.exporterConfig.StateKey, goutils.MergeMaps(currentEntitiesSet...))
	if err != nil {
		klog.Errorf("error deleting stale entities: %s", err.Error())
	}
	klog.Info("Done deleting stale entities")
}
