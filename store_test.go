package snowstore

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLocalStore_Set(t *testing.T) {
	s := &SnowStore{
		config: SnowStoreConfig{
			path: "./data",
		},
	}

	p := Person{
		ID:   1,
		Name: "Mike",
	}

	var res Person
	err := s.Get("", fmt.Sprintf("%d", p.ID), &res)
	assert.ErrorIs(t, ErrEntryNotFound, err)

	err = s.Set("", fmt.Sprintf("%d", p.ID), p)
	assert.NoError(t, err)

	err = s.Get("", fmt.Sprintf("%d", p.ID), &res)
	assert.NoError(t, err)
	assert.Equal(t, p, res)

	err = s.Delete("", fmt.Sprintf("%d", p.ID))
	assert.NoError(t, err)

	err = s.Get("", fmt.Sprintf("%d", p.ID), &res)
	assert.ErrorIs(t, ErrEntryNotFound, err)
}

func TestLocalStoreWithParent(t *testing.T) {
	s := &SnowStore{
		config: SnowStoreConfig{
			path: "./data",
		},
	}

	p := Person{
		ID:   1,
		Name: "Mike",
	}

	var res Person
	rand.Seed(time.Now().UnixNano())
	parent := fmt.Sprintf("%d", rand.Intn(1234567890))

	err := s.Get(parent, fmt.Sprintf("%d", p.ID), &res)
	assert.ErrorIs(t, ErrEntryNotFound, err)

	err = s.Set(parent, fmt.Sprintf("%d", p.ID), p)
	assert.NoError(t, err)

	err = s.Get(parent, fmt.Sprintf("%d", p.ID), &res)
	assert.NoError(t, err)
	assert.Equal(t, p, res)

	err = s.Delete(parent, fmt.Sprintf("%d", p.ID))
	assert.NoError(t, err)

	err = s.Get(parent, fmt.Sprintf("%d", p.ID), &res)
	assert.ErrorIs(t, ErrEntryNotFound, err)
}

func TestLocalStore_GetByParent(t *testing.T) {
	s := &SnowStore{
		config: SnowStoreConfig{
			path: "./data",
		},
	}

	p1 := Person{
		ID:   1,
		Name: "Mike",
	}

	p2 := Person{
		ID:   2,
		Name: "John",
	}

	rand.Seed(time.Now().UnixNano())
	parent := fmt.Sprintf("%d", rand.Intn(1234567890))

	it, err := s.GetByParent(parent)
	assert.NoError(t, err)
	assert.False(t, it.HasNext())

	_ = s.Set(parent, fmt.Sprintf("%d", p1.ID), p1)
	_ = s.Set(parent, fmt.Sprintf("%d", p2.ID), p2)

	it, err = s.GetByParent(parent)
	assert.NoError(t, err)

	cnt := 0

	for it.HasNext() {
		var res Person
		err := it.Next(&res)
		assert.NoError(t, err)

		if res.ID != p1.ID && res.ID != p2.ID {
			t.Error("invalid ID")
		}

		cnt += 1
	}

	assert.Equal(t, 2, cnt)
	err = s.DeleteByParent(parent)
	assert.NoError(t, err)

	it, err = s.GetByParent(parent)
	assert.NoError(t, err)
	assert.False(t, it.HasNext())

}
