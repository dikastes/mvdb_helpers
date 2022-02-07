library(tidyverse)
library(magrittr)

works <- read_csv('typo3_table_tx_publisherdb_domain_model_work.csv')
workform <- read_csv('typo3_table_tx_publisherdb_work_form_mm.csv')
form <- read_csv('typo3_table_tx_publisherdb_domain_model_form.csv')
workinstrument <- read_csv('typo3_table_tx_publisherdb_work_instrument_mm.csv')
instrument <- read_csv('typo3_table_tx_publisherdb_domain_model_instrument.csv')

genrelinks <- works %>%
	select(uid) %>%
	left_join( workform %>% rename (uid = uid_local) ) %>%
	left_join( form %>% rename (uid_foreign = uid, gnd_form = gnd_id) ) %>%
	distinct( uid, gnd_form ) %>%
	group_by(uid) %>%
	summarise( 
		genre_ids = str_c(gnd_form, collapse = '$')
	)

instrumentlinks <- works %>%
	select(uid) %>%
	left_join( workinstrument %>% rename (uid = uid_local) ) %>%
	left_join( instrument %>% rename (uid_foreign = uid, gnd_instrument = gnd_id) ) %>%
	distinct( uid, gnd_instrument ) %>%
	group_by(uid) %>%
	summarise( 
		instrument_ids = str_c(gnd_instrument, collapse = '$')
	)

cmd_gen <- function (uid, gnd, typein, typeout) {
	cmd <- "update `tx_publisherdb_domain_model_"
	cmd <- str_c(cmd, typein)
	cmd <- str_c(cmd, "` set `")
	cmd <- str_c(cmd, typeout)
	cmd <- str_c(cmd, "_ids` = '")
	cmd <- str_c(cmd, gnd)
	cmd <- str_c(cmd, "' where `uid` = ")
	cmd <- str_c(cmd, uid)
	cmd
}

cmd_gen <- Vectorize(cmd_gen)

commandform <- genrelinks %>%
	mutate( cmd = cmd_gen(uid, genre_ids, 'form', 'genre') ) %>%
	select( cmd ) %>%
	summarise( cmd = str_c(cmd, "\n") )

#commandform <- str_c( 'use `db`\n', commandform )

commandinstrument <- instrumentlinks %>%
	mutate( cmd = cmd_gen(uid, instrument_ids, 'instrument', 'instrument') ) %>%
	select( cmd ) %>%
	summarise( cmd = str_c(cmd, "\n") )

#commandinstrument <- str_c( 'use `db`\n', commandinstrument )

write_csv(commandform, 'command_form.csv')
write_csv(commandinstrument, 'command_instrument.csv')
