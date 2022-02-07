library(tidyverse)
d <- read_csv('mvdb_genres.csv')
sub <- d %>%select(sub)
super <- d %>%select(super)

d %>% filter(sub_name != 'Lied') %>% left_join( d %>% select(super = sub, na = sub_name) ) %>% filter(is.na(na))
super %>% right_join (sub %>% rename(super = sub))

ids <- sub %>% rbind(super %>% rename(sub = super)) %>% distinct %>% add_column(there = 'yes')

forms <- read_csv('typo3_table_tx_publisherdb_domain_model_form.csv')
forms %>% left_join(ids %>% rename(gnd_id = sub)) %>% filter(there != 'yes')

ids %>% left_join(forms %>% rename(sub = gnd_id)) %>% filter(is.na(uid))
