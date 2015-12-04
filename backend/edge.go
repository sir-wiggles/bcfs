package backend

type Edges map[string]map[string]*Properties

func (e *Edges) GetEdgeByID(fid, tid string) *Properties {
	edge := (*e)[fid][tid]
	if edge == nil {
		edge = &Properties{}
		(*e)[fid][tid] = edge
	}
	return edge
}
