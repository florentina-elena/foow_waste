library(httr)
library(jsonlite)
library(dplyr)
library(DBI)
library(RMariaDB)

api <- "SG_APIM_Z50GZHZDK97S73DAKNYQ0HPKS4P2CKWPF3YVHDCJ9PWW1AG6VQA0"
postnumre <- c(3400, 3200, 2840, 2400)

alle_clearances <- data.frame()

for (zip in postnumre) {
  url <- paste0("https://api.sallinggroup.com/v1/food-waste/?zip=", zip)
  dat <- fromJSON(content(GET(url, add_headers(Authorization = paste("Bearer", api))), 
                          "text"), flatten = TRUE)
  
  if (nrow(dat) == 0) next
  
  clear <- bind_rows(dat$clearances)
  clear$store_id   <- rep(dat$store.id, times = sapply(dat$clearances, nrow))
  clear$store_name <- rep(dat$store.name, times = sapply(dat$clearances, nrow))
  clear$zip        <- zip
  
  alle_clearances <- bind_rows(alle_clearances, clear)
}

print(nrow(alle_clearances))

# CONNECT
con <- dbConnect(
  MariaDB(),
  user = "dallocal",
  password = "Danielabuse22!",
  host = "localhost",
  dbname = "foodwaste"
)

# KUN DISCOUNT TABLE (ingen store!)
discount <- alle_clearances %>%
  transmute(
    store_id = store_id,
    ean = product.ean,
    product_name = product.description,
    new_price = offer.newPrice,
    original_price = offer.originalPrice,
    discount_percent = offer.percentDiscount,
    quantity = offer.stock,
    run_timestamp = Sys.time()
  )

dbWriteTable(con, "discount_products", discount, append = TRUE, row.names = FALSE)

# LOGFIL
write(
  paste(Sys.time(), "- cron kørsel - hentet", nrow(alle_clearances), "tilbud"),
  file = "/home/ubuntu/git/foow_waste/foodwaste_log.txt",
  append = TRUE
)

dbDisconnect(con)
