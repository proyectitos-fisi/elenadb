package common

/*
 Channels allow for safe sharing of data between threads. This is a multi-producer multi-consumer channel.
*/

import (
	"sync"
)

type Channel[T any] struct {
	mu    sync.Mutex // mutex para manejar la exclusión mutua
	cond  *sync.Cond // condition variable para manejar la sincronización
	queue []T        // almacena los elementos
}

func NewChannel[T any]() *Channel[T] {
	ch := &Channel[T]{
		queue: make([]T, 0),
	}
	ch.cond = sync.NewCond(&ch.mu)
	return ch
}

// inserta elemento en la cola.
func (ch *Channel[T]) Put(element T) {
	ch.mu.Lock()
	ch.queue = append(ch.queue, element)
	ch.mu.Unlock()
	ch.cond.Broadcast()
}

// extrae un elemento de la cola de manera segura.
func (ch *Channel[T]) Get() T {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	for len(ch.queue) == 0 {
		ch.cond.Wait() // si la cola está vacía, el hilo se bloquea y espera hasta que un nuevo elemento esté disponible.  utiliza la condition variable para esta sincronización.
	}
	element := ch.queue[0]
	ch.queue = ch.queue[1:]
	return element
}
