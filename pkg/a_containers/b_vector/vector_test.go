package vector

import (
	"github.com/stretchr/testify/require"
	"testing"
	"tiny_planner/pkg/a_containers/a_types"
)

func Test1(t *testing.T) {
	vec := NewVec(types.types.T_int32.ToType())
	err := vec.Append(int32(1), false)
	require.NoError(t, err)

	err = vec.Append(int32(2), false)
	require.NoError(t, err)

	err = vec.Append(int32(0), true)
	require.NoError(t, err)

	v, null := Get[int32](vec, 0)
	require.Equal(t, int32(1), v)

	v, null = Get[int32](vec, 1)
	require.Equal(t, int32(2), v)

	v, null = Get[int32](vec, 2)
	require.Equal(t, true, null)
}
