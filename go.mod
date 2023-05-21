module simple-db

replace (
	file_manager => ./file_manager
	log_manager => ./log_manager
	buffer_manager => ./buffer_manager
	tx => ./tx
)
go 1.19

 require (
 	file_manager v0.0.0
 	log_manager v0.0.0
 	buffer_manager v0.0.0
 	tx v0.0.0
 )