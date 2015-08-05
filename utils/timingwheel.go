// 通过channel这种close broadcast机制，我们可以非常方便的实现一个timer，
// timer有一个channel ch，所有需要在某一个时间 “T” 收到通知的goroutine都可以尝试读该ch，
// 当T到达时候，close该ch，那么所有的goroutine都能收到该事件了。
// timingwheel的使用很简单，首先我们创建一个wheel
// 这里我们创建了一个timingwheel，精度是1s，最大的超时等待时间为3600s
// w := timingwheel.NewTimingWheel(1 * time.Second, 3600)
// 等待10s
// <-w.After(10 * time.Second)
// 因为timingwheel只有一个1s的ticker，并且只创建了3600个channel，系统开销很小。

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

/*
func main() {
	var wg sync.WaitGroup
	var max_waiting int = 120
	var num_gorouting int = 1000

	rand.Seed(79)
	// 初始化时间轮定时器，精度为1秒，最大可等待120秒
	wheel := NewTimingWheel(1*time.Second, max_waiting)

	// 启动1000个goroutine
	for i := 0; i < num_gorouting; i++ {
		wg.Add(1)
		go func(count int) {
			// 随机定时0~120秒
			rand_sleep := rand.Int31() % max_waiting
			sleep := time.Duration(rand_sleep) * time.Second

			fmt.Printf("%d wait for %s seconds\n", count, sleep.String())
			<-wheel.After(sleep)
			fmt.Printf("%d was done, waited for %s seconds\n", count, sleep.String())

			wg.Done()
		}(i)
	}

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	wg.Wait()
}
*/
