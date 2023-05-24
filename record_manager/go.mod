module record_manager

go 1.19

replace (
	buffer_manager => ../buffer_manager
	file_manager => ../file_manager
	log_manager => ../log_manager
	tx => ../tx
)

require (
	buffer_manager v0.0.0 // indirect
	file_manager v0.0.0
	log_manager v0.0.0 // indirect
	tx v0.0.0
)

require github.com/stretchr/testify v1.8.3

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
