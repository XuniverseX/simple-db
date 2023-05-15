module simple-db

replace file_manager => ./file_manager

replace log_manager => ./log_manager

go 1.19

 require (
 	file_manager v0.0.0
 	log_manager v0.0.0
 )