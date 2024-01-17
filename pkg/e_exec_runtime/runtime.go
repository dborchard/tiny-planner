package execution

type RuntimeEnv struct {
	MemoryPool *MemPool
	//DiskManager DiskManager
	//CacheManager CacheManager
	//ObjectStoreRegistry ObjectStoreRegistry
}

func NewRuntimeEnv() *RuntimeEnv {
	return &RuntimeEnv{
		MemoryPool: &MemPool{},
	}
}
