module simple-db

replace (
	buffer_manager => ./buffer_manager
	file_manager => ./file_manager
	log_manager => ./log_manager
	tx => ./tx
)

go 1.19

require (
	buffer_manager v0.0.0
	file_manager v0.0.0
	log_manager v0.0.0
	tx v0.0.0
)
