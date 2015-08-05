package utils

import (
	"sync"
	"time"
)

type TimingWheel struct {
	// 互斥锁
	sync.Mutex

	// 定时精度
	interval time.Duration

	// 时间轮内部的定时器
	ticker *time.Ticker

	// 退出channel
	quit chan struct{}

	// 最大支持的等待时间
	maxTimeout time.Duration

	// 所有时间轮上所有的刻度
	cs []chan struct{}

	// 当前时间轮上所指的刻度
	pos int
}

// 初始化新的时间轮对象
// @interval 定时精度
// @buckets 最大支持的等待时间
func NewTimingWheel(interval time.Duration, buckets int) *TimingWheel {
	w := new(TimingWheel)

	w.interval = interval

	w.quit = make(chan struct{})
	w.pos = 0

	w.maxTimeout = time.Duration(interval * (time.Duration(buckets)))

	w.cs = make([]chan struct{}, buckets)

	// 初始化所有的buckets
	for i := range w.cs {
		w.cs[i] = make(chan struct{})
	}

	// 启动循环定时器，并指定interval为精度
	w.ticker = time.NewTicker(interval)
	// 启动时间轮
	go w.run()

	return w
}

func (w *TimingWheel) Stop() {
	close(w.quit)
}

// 返回一个channel，goroutine在其上读取就会阻塞
// 等到轮盘上的指针指向对应的刻度，这个channel就会被关闭
// goroutine的阻塞就会被取消
func (w *TimingWheel) After(timeout time.Duration) <-chan struct{} {
	if timeout >= w.maxTimeout {
		panic("timeout too much, over maxtimeout")
	}

	w.Lock()

	// 计算参数timeout在buckets中的索引
	index := (w.pos + int(timeout/w.interval)) % len(w.cs)

	// 返回这个索引保存的channel
	b := w.cs[index]

	w.Unlock()

	return b
}

// 启动时间轮内部的定时器
func (w *TimingWheel) run() {
	for {
		select {
		case <-w.ticker.C:
			w.onTicker()
		case <-w.quit:
			w.ticker.Stop()
			return
		}
	}
}

/*
时间轮算法的关键两点是：
1. 每隔单位时间指针指向轮盘上的其中一个刻度
2. 挂接到这个刻度上的线程都会得到通知
*/
func (w *TimingWheel) onTicker() {
	w.Lock()

	// 获取当前刻度的channel
	lastC := w.cs[w.pos]

	// 为当前刻度重新分配新的channel
	w.cs[w.pos] = make(chan struct{})

	// 刻度前进1
	w.pos = (w.pos + 1) % len(w.cs)

	w.Unlock()

	// 从关闭的channel读取的任何goroutine永远不会阻塞
	// 所以这里起到了一个通知等待在这个channel上的所有goroutine的作用
	close(lastC)
}
