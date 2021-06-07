package framework

import (
	"math"
	"sort"

	"github.com/TheWeatherCompany/icm-redis-operator/pkg/api/redis/v1alpha1"
	"github.com/TheWeatherCompany/icm-redis-operator/pkg/redis"
	corev1 "k8s.io/api/core/v1"
)

func getZonesFromKubeNodes(nodes []corev1.Node) []string {
	set := make(map[string]struct{})
	var zones []string
	for _, node := range nodes {
		zone, ok := node.Labels[corev1.LabelTopologyZone]
		if ok {
			if _, hasZone := set[zone]; !hasZone {
				set[zone] = struct{}{}
			}
		}

	}
	if len(set) == 0 {
		set[redis.UnknownZone] = struct{}{}
	}
	for key := range set {
		zones = append(zones, key)
	}
	sort.Strings(zones)
	return zones
}

func getZone(nodeName string, kubeNodes []corev1.Node) string {
	for _, node := range kubeNodes {
		if node.Name == nodeName {
			label, ok := node.Labels[corev1.LabelTopologyZone]
			if ok {
				return label
			} else {
				return redis.UnknownZone
			}
		}
	}
	return redis.UnknownZone
}

func getZoneSkew(zoneToNodes map[string][]v1alpha1.RedisClusterNode) int {
	if len(zoneToNodes) == 0 {
		return 0
	}
	largestZoneSize := 0
	smallestZoneSize := math.MaxInt32
	for _, nodes := range zoneToNodes {
		if len(nodes) > largestZoneSize {
			largestZoneSize = len(nodes)
		}
		if len(nodes) < smallestZoneSize {
			smallestZoneSize = len(nodes)
		}
	}
	return largestZoneSize - smallestZoneSize
}

func zonesSkewed(zoneToPrimaries map[string][]v1alpha1.RedisClusterNode, zoneToReplicas map[string][]v1alpha1.RedisClusterNode) error {
	primarySkew := getZoneSkew(zoneToPrimaries)
	replicaSkew := getZoneSkew(zoneToReplicas)
	if primarySkew > 2 {
		return LogAndReturnErrorf("primary node zones are not balanced, skew is too large: %v", primarySkew)
	}
	if replicaSkew > 2 {
		return LogAndReturnErrorf("replica node zones are not balanced, skew is too large: %v", replicaSkew)
	}
	return nil
}

func sameZone(node v1alpha1.RedisClusterNode, zone string, idToPrimary map[string]v1alpha1.RedisClusterNode, kubeNodes []corev1.Node) bool {
	if primary, ok := idToPrimary[node.PrimaryRef]; ok {
		if primary.Pod != nil {
			primaryZone := getZone(primary.Pod.Spec.NodeName, kubeNodes)
			if zone == primaryZone {
				return true
			}
		}
	}
	return false
}

func addNodeToMaps(node v1alpha1.RedisClusterNode, nodeName string, kubeNodes []corev1.Node, idToPrimary map[string]v1alpha1.RedisClusterNode, zoneToPrimaries, zoneToReplicas map[string][]v1alpha1.RedisClusterNode) {
	nodeZone := getZone(nodeName, kubeNodes)
	if node.Role == v1alpha1.RedisClusterNodeRolePrimary {
		zoneToPrimaries[nodeZone] = append(zoneToPrimaries[nodeZone], node)
		idToPrimary[node.ID] = node
	}
	if node.Role == v1alpha1.RedisClusterNodeRoleReplica {
		zoneToReplicas[nodeZone] = append(zoneToReplicas[nodeZone], node)
	}
}
