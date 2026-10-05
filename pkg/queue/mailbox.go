package queue

const completedWorkIDLimit = 65536

type completedWorkIDs struct {
	limit   int
	entries map[string]struct{}
	order   []string
	next    int
}

func newCompletedWorkIDs(limit int) completedWorkIDs {
	if limit < 1 {
		limit = completedWorkIDLimit
	}
	return completedWorkIDs{limit: limit, entries: make(map[string]struct{}), order: make([]string, 0, limit)}
}

func (ids *completedWorkIDs) contains(id string) bool {
	_, exists := ids.entries[id]
	return exists
}

func (ids *completedWorkIDs) add(id string) {
	if id == "" || ids.contains(id) {
		return
	}
	if len(ids.order) < ids.limit {
		ids.order = append(ids.order, id)
		ids.entries[id] = struct{}{}
		return
	}
	delete(ids.entries, ids.order[ids.next])
	ids.order[ids.next] = id
	ids.entries[id] = struct{}{}
	ids.next = (ids.next + 1) % ids.limit
}
