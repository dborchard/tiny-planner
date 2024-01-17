package execution

type IMemPool interface {
	Alloc(size int) []byte
	Free(buf []byte)
}

var _ IMemPool = &MemPool{}

type MemPool struct {
}

func (m *MemPool) Alloc(size int) []byte {
	//TODO implement me
	panic("implement later")
}

func (m *MemPool) Free(buf []byte) {
	//TODO implement me
	panic("implement later")
}
