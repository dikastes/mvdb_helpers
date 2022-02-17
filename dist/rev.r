library(tidyverse)
library(magrittr)

genres <- read_csv('14-2.csv', col_names = FALSE)
colnames(genres) <- c('sub', 'super')
insts <- read_csv('14-3.csv', col_names = FALSE)
colnames(insts) <- c('sub', 'super')
decoder <- read_csv('14.1.csv', col_names = FALSE) %>%
	rbind( read_csv('14.2.csv', col_names = FALSE) ) %>%
	rbind( read_csv('14.3.csv', col_names = FALSE) ) %>%
	rbind( read_csv('14.4.csv', col_names = FALSE) ) %>%
	select( X1, X2 )
colnames(decoder) <- c('id', 'name')
decoder %<>%
	mutate(
		name = str_replace_all(name, regex("[ \\-']"), '_')
	)


genres %<>%
	left_join(decoder %>% rename(sub = name)) %>%
	rename(sub = id, sub_name = sub) %>%
	left_join(decoder %>% rename(super = name)) %>%
	rename(super = id, super_name = super)
insts %<>%
	left_join(decoder %>% rename(sub = name)) %>%
	rename(sub = id, sub_name = sub) %>%
	left_join(decoder %>% rename(super = name)) %>%
	rename(super = id, super_name = super)

write_csv(genres, 'mvdb_genres.csv')
write_csv(insts, 'mvdb_instruments.csv')
