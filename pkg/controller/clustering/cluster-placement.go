package clustering

import (
	"fmt"
	"math"
	"sort"

	"github.com/IBM/operator-for-redis-cluster/pkg/redis"
	"github.com/golang/glog"
)

const (
	unknownNodeName = "unknown" // I hope nobody will ever name a node "unknown" because this will impact the algorithm inside this package
)

// PlacePrimaries selects primary redis nodes by spreading out the primaries across zones as much as possible.
func PlacePrimaries(cluster *redis.Cluster, currentPrimaries, candidatePrimaries redis.Nodes, nbPrimary int32) (redis.Nodes, bool, error) {
	selection := redis.Nodes{}
	selection = append(selection, currentPrimaries...)
	zones := cluster.GetZones()

	// in case of scale down, we use the required number of primaries instead of
	// current number of primaries to limit the size of the selection
	if len(selection) > int(nbPrimary) {
		selectedPrimaries, err := selectPrimariesByZone(zones, selection, nbPrimary)
		if err != nil {
			glog.Errorf("Error selecting primaries by zone: %v", err)
		}
		selection = selectedPrimaries
	}

	nodeToCurrentPrimaries := k8sNodeToRedisNodes(cluster, currentPrimaries)
	nodeToCandidatePrimaries := k8sNodeToRedisNodes(cluster, candidatePrimaries)

	return addPrimaries(zones, selection, nodeToCurrentPrimaries, nodeToCandidatePrimaries, nbPrimary)
}

func getOptimalNodeIndex(zones []string, nodes, candidates redis.Nodes) int {
	// iterate over candidate nodes and choose one that preserves zone balance
	for i, candidate := range candidates {
		if ZonesBalanced(zones, candidate, nodes) {
			glog.V(4).Infof("- add node: %s to the selection", candidate.ID)
			return i
		}
	}
	return -1
}

func selectOptimalPrimaries(bestEffort bool, zones []string, selection redis.Nodes, nodeToCurrentPrimaries map[string]redis.Nodes, nodeToCandidatePrimaries map[string]redis.Nodes, nbPrimary int32) (bool, redis.Nodes) {
	optimalPrimaries := redis.Nodes{}
	nodeAdded := false
	for nodeID, candidates := range nodeToCandidatePrimaries {
		// discard kubernetes nodes with primaries when we are not in best effort mode
		if _, ok := nodeToCurrentPrimaries[nodeID]; !bestEffort && ok {
			continue
		}
		index := getOptimalNodeIndex(zones, selection, candidates)
		if index != -1 {
			nodeAdded = true
			optimalPrimaries = append(optimalPrimaries, candidates[index])
			candidates[index] = candidates[len(candidates)-1]
			nodeToCandidatePrimaries[nodeID] = candidates[:len(candidates)-1]
			if len(selection)+len(optimalPrimaries) >= int(nbPrimary) {
				return nodeAdded, optimalPrimaries
			}
		}
	}
	return nodeAdded, optimalPrimaries
}

func selectBestEffortPrimaries(selection redis.Nodes, nodeToCandidatePrimaries map[string]redis.Nodes, nbPrimary int32) redis.Nodes {
	bestEffortPrimaries := redis.Nodes{}
	for _, candidates := range nodeToCandidatePrimaries {
		for _, candidate := range candidates {
			glog.V(4).Infof("- add node: %s to the selection", candidate.ID)
			bestEffortPrimaries = append(bestEffortPrimaries, candidate)
			if len(selection)+len(bestEffortPrimaries) >= int(nbPrimary) {
				return bestEffortPrimaries
			}
		}
	}
	return bestEffortPrimaries
}

func selectNewPrimaries(zones []string, selection redis.Nodes, nodeToCurrentPrimaries map[string]redis.Nodes, nodeToCandidatePrimaries map[string]redis.Nodes, nbPrimary int32) (redis.Nodes, bool) {
	bestEffort := false
	// iterate until we have the requested number of primaries
	for len(selection) < int(nbPrimary) {
		// select optimal primaries that preserve zone balance
		nodeAdded, optimalSelection := selectOptimalPrimaries(bestEffort, zones, selection, nodeToCurrentPrimaries, nodeToCandidatePrimaries, nbPrimary)
		selection = append(selection, optimalSelection...)
		if len(selection) >= int(nbPrimary) {
			return selection, bestEffort
		}
		if bestEffort && !nodeAdded {
			glog.Infof("no remaining optimal primaries, entering best effort mode")
			// select any available primaries
			bestEffortSelection := selectBestEffortPrimaries(selection, nodeToCandidatePrimaries, nbPrimary)
			selection = append(selection, bestEffortSelection...)
			break
		}
		bestEffort = true
	}
	return selection, bestEffort
}

func addPrimaries(zones []string, primaries redis.Nodes, nodeToCurrentPrimaries map[string]redis.Nodes, nodeToCandidatePrimaries map[string]redis.Nodes, nbPrimary int32) (redis.Nodes, bool, error) {
	newPrimaries, bestEffort := selectNewPrimaries(zones, primaries, nodeToCurrentPrimaries, nodeToCandidatePrimaries, nbPrimary)
	glog.Infof("- bestEffort %v", bestEffort)
	for _, primary := range newPrimaries {
		glog.Infof("- primary %s, ip: %s", primary.ID, primary.IP)
	}
	if len(newPrimaries) >= int(nbPrimary) {
		return newPrimaries, bestEffort, nil
	}
	return newPrimaries, bestEffort, fmt.Errorf("insufficient number of redis nodes for the requested number of primaries")
}

func ZoneToNodes(zones []string, nodes redis.Nodes) map[string]redis.Nodes {
	zoneToNodes := make(map[string]redis.Nodes)
	for _, zone := range zones {
		zoneToNodes[zone] = redis.Nodes{}
	}
	for _, node := range nodes {
		zoneToNodes[node.Zone] = append(zoneToNodes[node.Zone], node)
	}
	return zoneToNodes
}

func selectPrimariesByZone(zones []string, primaries redis.Nodes, nbPrimary int32) (redis.Nodes, error) {
	selection := redis.Nodes{}
	zoneToNodes := ZoneToNodes(zones, primaries)
	for len(selection) < int(nbPrimary) {
		for zone, nodes := range zoneToNodes {
			if len(nodes) > 0 {
				selection = append(selection, nodes[0])
				zoneToNodes[zone] = nodes[1:]
			}
			if len(selection) >= int(nbPrimary) {
				return selection, nil
			}
		}
	}
	return selection, fmt.Errorf("insufficient number of redis nodes for the requested number of primaries")
}

func ZonesBalanced(zones []string, candidate *redis.Node, nodes redis.Nodes) bool {
	if len(nodes) == 0 {
		return true
	}
	zoneToNodes := ZoneToNodes(zones, nodes)
	smallestZoneSize := math.MaxInt32
	for _, zoneNodes := range zoneToNodes {
		if len(zoneNodes) < smallestZoneSize {
			smallestZoneSize = len(zoneNodes)
		}
	}
	return len(zoneToNodes[candidate.Zone]) <= smallestZoneSize
}

// PlaceReplicas selects replica redis nodes for each primary by spreading out the replicas across zones as much as possible.
func PlaceReplicas(cluster *redis.Cluster, primaryToReplicas map[string]redis.Nodes, newReplicas, unusedReplicas redis.Nodes, replicationFactor int32) error {
	zoneToReplicas := ZoneToNodes(cluster.GetZones(), newReplicas)
	addReplicasToZones(zoneToReplicas, unusedReplicas)
	return addReplicasToPrimariesByZone(cluster, primaryToReplicas, zoneToReplicas, replicationFactor)
}

func containsReplica(replicas redis.Nodes, replica *redis.Node) bool {
	for _, r := range replicas {
		if r.ID == replica.ID {
			return true
		}
	}
	return false
}

func RemoveOldReplicas(oldReplicas, newReplicas redis.Nodes) redis.Nodes {
	// be sure that no oldReplicas are present in newReplicas
	i := 0
	for _, newReplica := range newReplicas {
		if containsReplica(oldReplicas, newReplica) {
			glog.V(4).Infof("removing old replica with id %s from list of new replicas", newReplica.ID)
		} else {
			newReplicas[i] = newReplica
			i++
		}
	}
	return newReplicas[:i]
}

func GeneratePrimaryToReplicas(primaries, replicas redis.Nodes, replicationFactor int32) (map[string]redis.Nodes, redis.Nodes) {
	primaryToReplicas := make(map[string]redis.Nodes)
	unusedReplicas := redis.Nodes{}
	for _, node := range primaries {
		primaryToReplicas[node.ID] = redis.Nodes{}
	}
	// build primary to replicas map and save all unused replicas
	for _, replica := range replicas {
		nodeAdded := false
		for _, primary := range primaries {
			if replica.PrimaryReferent == primary.ID {
				if len(primaryToReplicas[primary.ID]) < int(replicationFactor) {
					// primary of this replica is among the new primary nodes
					primaryToReplicas[primary.ID] = append(primaryToReplicas[primary.ID], replica)
					nodeAdded = true
				}
				break
			}
		}
		if !nodeAdded {
			unusedReplicas = append(unusedReplicas, replica)
		}
	}
	return primaryToReplicas, unusedReplicas
}

func addReplicasToPrimariesByZone(cluster *redis.Cluster, primaryToReplicas map[string]redis.Nodes, zoneToReplicas map[string]redis.Nodes, replicationFactor int32) error {
	zones := cluster.GetZones()
	for primaryID, currentReplicas := range primaryToReplicas {
		if len(currentReplicas) < int(replicationFactor) {
			primary, err := cluster.GetNodeByID(primaryID)
			if err != nil {
				glog.Errorf("unable to find primary with id: %s", primaryID)
				break
			}
			if err = addReplicasToPrimary(zones, primary, primaryToReplicas, zoneToReplicas, replicationFactor); err != nil {
				return err
			}
		}
	}
	return nil
}

func SameZone(zone string, nodes redis.Nodes) bool {
	for _, node := range nodes {
		if zone == node.Zone {
			return true
		}
	}
	return false
}

func nodeIndex(node *redis.Node, nodes []string) int {
	for i, nodeName := range nodes {
		if nodeName == node.Pod.Spec.NodeName {
			return i
		}
	}
	return 0
}

func isNodeNameUnique(node *redis.Node, nodes []string) bool {
	for _, nodeName := range nodes {
		if nodeName == node.Pod.Spec.NodeName {
			return false
		}
	}
	return true
}

func uniqueNodesInZone(primaryToReplicas map[string]redis.Nodes, zoneReplicas redis.Nodes, zone string) []string {
	var nodes []string
	for _, replica := range zoneReplicas {
		if isNodeNameUnique(replica, nodes) {
			nodes = append(nodes, replica.Pod.Spec.NodeName)
		}
	}
	for _, replicas := range primaryToReplicas {
		for _, replica := range replicas {
			if zone == replica.Zone && isNodeNameUnique(replica, nodes) {
				nodes = append(nodes, replica.Pod.Spec.NodeName)
			}
		}
	}
	sort.Strings(nodes)
	return nodes
}

func selectOptimalZoneReplicaIndex(primary *redis.Node, primaryToReplicas map[string]redis.Nodes, zoneReplicas redis.Nodes) int {
	numReplicas := len(primaryToReplicas[primary.ID])
	nodes := uniqueNodesInZone(primaryToReplicas, zoneReplicas, primary.Zone)
	primaryIndex := nodeIndex(primary, nodes)
	replicaIndex := (primaryIndex + numReplicas + 1) % len(nodes)
	replicaNodeName := nodes[replicaIndex]
	zoneReplicaIndex := 0
	replicaType := "suboptimal"
	zoneReplica := zoneReplicas[0]
	for i, replica := range zoneReplicas {
		if replicaNodeName == replica.Pod.Spec.NodeName {
			zoneReplicaIndex = i
			replicaType = "optimal"
			zoneReplica = replica
			break
		}
	}
	glog.V(4).Infof("found replica (%s) %s for primary %s on node %s. primary is on %s. primary index: %d, replica index: %d, replica node name: %s, nodes: %v",
		replicaType, zoneReplica.ID, primary.ID, zoneReplica.Pod.Spec.NodeName, primary.Pod.Spec.NodeName, primaryIndex, replicaIndex, replicaNodeName, nodes)
	return zoneReplicaIndex
}

func removeReplica(slice redis.Nodes, s int) redis.Nodes {
	return append(slice[:s], slice[s+1:]...)
}

func selectOptimalReplicas(zones []string, primary *redis.Node, primaryToReplicas map[string]redis.Nodes, zoneToReplicas map[string]redis.Nodes, replicationFactor int32) bool {
	zoneIndex := GetZoneIndex(zones, primary.Zone, primaryToReplicas[primary.ID])
	nodeAdded := false
	i := 0
	for i < len(zones) && len(primaryToReplicas[primary.ID]) < int(replicationFactor) {
		zone := zones[zoneIndex]
		zoneReplicas := zoneToReplicas[zone]
		zoneIndex = (zoneIndex + 1) % len(zones)
		if len(zoneReplicas) > 0 {
			sort.Slice(zoneReplicas, func(i, j int) bool {
				return zoneReplicas[i].Pod.Spec.NodeName < zoneReplicas[j].Pod.Spec.NodeName
			})
			// if RF < # of zones, we can achieve optimal placement
			if int(replicationFactor) < len(zones) {
				// skip this zone if it is the same as the primary zone or if this primary already has replicas in this zone
				if primary.Zone == zone || SameZone(zone, primaryToReplicas[primary.ID]) {
					continue
				}
			}
			nodeAdded = true
			zoneReplicaIndex := selectOptimalZoneReplicaIndex(primary, primaryToReplicas, zoneReplicas)
			glog.V(4).Infof("adding replica %s to primary %s", zoneReplicas[zoneReplicaIndex].ID, primary.ID)
			primaryToReplicas[primary.ID] = append(primaryToReplicas[primary.ID], zoneReplicas[zoneReplicaIndex])
			zoneToReplicas[zone] = removeReplica(zoneReplicas, zoneReplicaIndex)
		}
		i++
	}
	return nodeAdded
}

func usedNodesByShard(primary *redis.Node, primaryToReplicas map[string]redis.Nodes) []string {
	var nodes []string
	// Add k8s node for the primary pod
	nodes = append(nodes, primary.Pod.Spec.NodeName)

	// Add k8s nodes for replica pods
	for _, replica := range primaryToReplicas[primary.ID] {
		if isNodeNameUnique(replica, nodes) {
			nodes = append(nodes, replica.Pod.Spec.NodeName)
		}
	}
	return nodes
}

func selectBestEffortReplicas(zones []string, primary *redis.Node, primaryToReplicas map[string]redis.Nodes, zoneToReplicas map[string]redis.Nodes, replicationFactor int32) {
	zoneIndex := GetZoneIndex(zones, primary.Zone, primaryToReplicas[primary.ID])
	numEmptyZones := 0
	// iterate while zones are non-empty and the number of replicas is less than RF
	for numEmptyZones < len(zones) && len(primaryToReplicas[primary.ID]) < int(replicationFactor) {
		zone := zones[zoneIndex]
		zoneReplicas := zoneToReplicas[zone]
		zoneIndex = (zoneIndex + 1) % len(zones)
		selectedZoneReplicaIndex := 0
		if len(zoneReplicas) > 0 {
			// try assigning replica to a different k8s node than primary or other replicas
			for selectedZoneReplicaIndex < len(zoneReplicas) {
				if isNodeNameUnique(zoneReplicas[selectedZoneReplicaIndex], usedNodesByShard(primary, primaryToReplicas)) {
					break
				} else if selectedZoneReplicaIndex+1 < len(zoneReplicas) {
					selectedZoneReplicaIndex++
				}
			}
			glog.V(4).Infof("adding replica %s to primary %s", zoneReplicas[selectedZoneReplicaIndex].ID, primary.ID)
			primaryToReplicas[primary.ID] = append(primaryToReplicas[primary.ID], zoneReplicas[selectedZoneReplicaIndex])
			zoneToReplicas[zone] = append(zoneReplicas[:selectedZoneReplicaIndex], zoneReplicas[selectedZoneReplicaIndex+1:]...)
		} else {
			numEmptyZones++
		}
	}
}

func addReplicasToPrimary(zones []string, primary *redis.Node, primaryToReplicas map[string]redis.Nodes, zoneToReplicas map[string]redis.Nodes, replicationFactor int32) error {
	// iterate while the number of replicas is less than RF
	for len(primaryToReplicas[primary.ID]) < int(replicationFactor) {
		if !selectOptimalReplicas(zones, primary, primaryToReplicas, zoneToReplicas, replicationFactor) && len(primaryToReplicas[primary.ID]) < int(replicationFactor) {
			glog.Infof("no remaining optimal replicas, entering best effort mode")
			selectBestEffortReplicas(zones, primary, primaryToReplicas, zoneToReplicas, replicationFactor)
			break
		}
	}
	if len(primaryToReplicas[primary.ID]) < int(replicationFactor) {
		return fmt.Errorf("insufficient number of replicas for primary %s, expected: %v, actual: %v", primary.ID, int(replicationFactor), len(primaryToReplicas[primary.ID]))
	}
	return nil
}

func addReplicasToZones(zoneToReplicas map[string]redis.Nodes, replicas redis.Nodes) {
	for _, replica := range replicas {
		zoneToReplicas[replica.Zone] = append(zoneToReplicas[replica.Zone], replica)
	}
}

func GetZoneIndex(zones []string, primaryZone string, replicas redis.Nodes) int {
	for i, zone := range zones {
		if primaryZone == zone {
			return (i + len(replicas) + 1) % len(zones)
		}
	}
	return 0
}

// k8sNodeToRedisNodes returns a mapping from kubernetes nodes to a list of redis nodes that will be hosted on the node
func k8sNodeToRedisNodes(cluster *redis.Cluster, nodes redis.Nodes) map[string]redis.Nodes {
	nodeToRedisNodes := make(map[string]redis.Nodes)
	for _, rNode := range nodes {
		cNode, err := cluster.GetNodeByID(rNode.ID)
		if err != nil {
			glog.Errorf("[k8sNodeToRedisNodes] unable to find the node with redis ID:%s", rNode.ID)
			continue
		}
		nodeName := unknownNodeName
		if cNode.Pod != nil && cNode.Pod.Spec.NodeName != "" {
			nodeName = cNode.Pod.Spec.NodeName
		}
		if _, ok := nodeToRedisNodes[nodeName]; !ok {
			nodeToRedisNodes[nodeName] = redis.Nodes{}
		}
		nodeToRedisNodes[nodeName] = append(nodeToRedisNodes[nodeName], rNode)
	}

	return nodeToRedisNodes
}
