package schedule

func unique[V comparable](slice []V) []V {
	keys := make(map[V]bool)
	list := []V{}

	for _, entry := range slice {
		if _, value := keys[entry]; !value {
			keys[entry] = true
			list = append(list, entry)
		}
	}

	return list
}
