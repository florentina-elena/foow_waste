library(httr)
library(jsonlite)
library(dplyr)
library(DBI)
library(RMariaDB)


#####Hente for flere post nummer og får "clearences" fra tabellen altså får en data frame med produkterne

api <- "SG_APIM_Z50GZHZDK97S73DAKNYQ0HPKS4P2CKWPF3YVHDCJ9PWW1AG6VQA0"
postnumre <- c(3400, 3200, 2840, 2400)

alle_clearances <- data.frame()

for (zip in postnumre) {
  
  # Hent data
  url <- paste0("https://api.sallinggroup.com/v1/food-waste/?zip=", zip)
  dat <- fromJSON(content(GET(url, add_headers(Authorization = paste("Bearer", api))), 
                          "text"), 
                  flatten = TRUE)
  
  # Hvis ingen butikker
  if (nrow(dat) == 0) next
  
  # Pak alle butikkers clearances ud automatisk
  clear <- bind_rows(dat$clearances)
  
  # Tilføj stamdata (gentages automatisk for alle rækker)
  clear$store_id   <- rep(dat$store.id,   times = sapply(dat$clearances, nrow))
  clear$store_name <- rep(dat$store.name, times = sapply(dat$clearances, nrow))
  clear$zip        <- zip
  
  # Tilføj til samlet tabel
  alle_clearances <- bind_rows(alle_clearances, clear)
}

print(nrow(alle_clearances))
print(names(alle_clearances))


#Efter oprettelsen af data.base i mysql skal vi nu udfylde de 2 tabeller som vi har oprettet der:
stores <- alle_clearances %>%
  select(store_id, store_name, zip) %>%
  distinct()

dbWriteTable(con, "store", stores, append = TRUE, row.names = FALSE)

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




