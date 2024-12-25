package utils

import (
	"errors"

	"github.com/kentik/patricia"
	"github.com/kentik/patricia/generics_tree"
)

type NetworksTree[T any] struct {
	treeV4 *generics_tree.TreeV4[T]
	treeV6 *generics_tree.TreeV6[T]
}

func NewNetworksTree[T any]() *NetworksTree[T] {
	return &NetworksTree[T]{
		treeV4: generics_tree.NewTreeV4[T](),
		treeV6: generics_tree.NewTreeV6[T](),
	}
}

func (n *NetworksTree[T]) Set(ip string, value T) error {
	ipV4, ipV6, err := patricia.ParseIPFromString(ip)

	if err != nil {
		return err
	}

	if ipV4 != nil {
		n.treeV4.Set(*ipV4, value)
	} else {
		n.treeV6.Set(*ipV6, value)
	}

	return nil
}

func (n *NetworksTree[T]) Get(ip string) (T, error) {
	var value T

	ipV4, ipV6, err := patricia.ParseIPFromString(ip)

	if err != nil {
		return value, err
	}

	var found bool

	if ipV4 != nil {
		found, value = n.treeV4.FindDeepestTag(*ipV4)
	} else {
		found, value = n.treeV6.FindDeepestTag(*ipV6)
	}

	if !found {
		return value, errors.New("not found")
	}

	return value, nil
}

func (n *NetworksTree[T]) ForEach(callback func(string, T)) {
	iV4 := n.treeV4.Iterate()

	for iV4.Next() {
		value := iV4.Address()

		for _, tag := range iV4.Tags() {
			callback(value.String(), tag)
		}
	}

	iV6 := n.treeV6.Iterate()

	for iV6.Next() {
		value := iV6.Address()

		for _, tag := range iV6.Tags() {
			callback(value.String(), tag)
		}
	}
}
