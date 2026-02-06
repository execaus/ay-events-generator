package generator

type Mode string

// Режим генерации по умолчанию
const defaultMode Mode = RegularMode

// Режимы генерации событий
const (
	RegularMode  Mode = "regular" // Постоянный поток событий
	PickLoadMode      = "pick"    // Пиковая нагрузка
	NightMode         = "night"   // Ночные редкие события
)

// Вероятности генерации события для разных режимов
const (
	regularModeEventProb = 0.1
	pickLoadMinEvents    = 5
	pickLoadMaxEvents    = 50
	nightModeEventProb   = 0.01
)
