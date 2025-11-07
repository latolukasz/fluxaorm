package fluxaorm

import "strconv"

type Pager struct {
	CurrentPage int
	PageSize    int
}

func NewPager(currentPage, pageSize int) *Pager {
	return &Pager{
		CurrentPage: currentPage,
		PageSize:    pageSize,
	}
}

func (pager *Pager) GetPageSize() int {
	return pager.PageSize
}

func (pager *Pager) GetCurrentPage() int {
	return pager.CurrentPage
}

func (pager *Pager) IncrementPage() {
	pager.CurrentPage++
}

func (pager *Pager) paginateSlice(items []uint64) []uint64 {
	start, end := pager.cutsSlice(len(items))
	return items[start:end]
}
func (pager *Pager) cutsSlice(len int) (int, int) {
	start := (pager.CurrentPage - 1) * pager.PageSize
	if start >= len {
		return 0, 0
	}

	end := start + pager.PageSize
	if end > len {
		end = len
	}

	return start, end
}

func (pager *Pager) String() string {
	return "LIMIT " + strconv.Itoa((pager.CurrentPage-1)*pager.PageSize) + "," + strconv.Itoa(pager.PageSize)
}
