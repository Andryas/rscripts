#!/usr/bin/env Rscript

arg <- commandArgs(trailingOnly = TRUE)
arg <- ifelse(length(arg) == 0, "", arg)

# Function ----------------------------------------------------------------
superbidApi <- function() {
  cat("Get API information...\n")
  url <-
    "https://api.sbwebservices.net/offer-query/categories/?portalId=2"
  api <- jsonlite::fromJSON(url, FALSE)
  api <-
    lapply(api$productsType, function(x)
      dplyr::bind_rows(x$categories))
  api <- dplyr::bind_rows(api)[, 1:2]
  return(api)
}

superbidOrder <- function(orderDate = Sys.Date(), id = NULL) {
  cat("Get Orders...\n")
  api <- superbidApi()
  if (!is.null(id)) {
    api <- api %>% dplyr::filter(id %in% id)
  }
  cat("Creating auctions links...\n")
  links <- sapply(api$id, function(id) {
    paste0(
      "https://api.sbwebservices.net/offer-query/offers?portalId=2&",
      "filter=statusId%3A1%3Bproduct.subCategory.category.id%3A",
      id,
      "&searchType=opened&start=0&limit=10000&orderBy=lotNumber:asc"
    )
  })
  
  superbidOrders <- function(url, orderDate) {
    data <- jsonlite::fromJSON(url, FALSE)
    data <- data$offers
    index <-
      sapply(data, function(x)
        as.Date(x$endDate) == orderDate)
    data <- data[index]
    if (length(data) > 0) {
      ids <- sapply(data, function(x)
        x$id)
      Sys.sleep(2)
      return(ids)
    } else {
      Sys.sleep(2)
      return(NULL)
    }
  }
  cat("Getting auction links...\n")
  ids <- lapply(links, superbidOrders, orderDate)
  ids <- do.call(c, ids)
  cat("Done!\n")
  return(ids)
}

superbidData <- function(ids) {
  cat("Get data from completed auctions...\n")
  url <-
    "https://api.sbwebservices.net/offer-query/offers?portalId=2&filter=id:[%s]"
  
  ids.l <- split(ids, splitGroupEqual(length(ids), ceiling(length(ids) / 50)))
  
  cat("Getting the data...\n\n")
  data <- lapply(ids.l, function(x) {
    idList <- paste0(x, collapse = ",")
    data <-
      jsonlite::fromJSON(sprintf(url, idList), simplifyVector = FALSE)
    data <- data$offers
    return(data)
  })
  
  data <- unlist(data, recursive = FALSE)
  cat("Done!\n")
  return(data)
}

# Crontab -----------------------------------------------------------------
if (arg == "crontab" | arg == "cron") {
  library(cronR)
  cmd1 <- cron_rscript("superbid.R", rscript_args = c("order"))
  cron_add(
    cmd1,
    "daily",
    "7:00",
    id = "superbidOrder",
    tags = "webscraping",
    description = "Collect the order auctiones."
  )
  cmd2 <- cron_rscript("superbid.R", rscript_args = c("data"))
  cron_add(
    cmd2,
    "daily",
    "17:30",
    id = "superbidData",
    tags = "webscraping",
    description = "Collect the day's data."
  )
} else if (arg == "order" | arg == "data") {
  con <- tempfile()
  sink(con, append = TRUE)
  
  cat(
    paste0(
      "# ",
      paste0(rep("=", 50), collapse = ""),
      "\n# Superbid\n# ",
      date(),
      "\n",
      "# ",
      paste0(rep("=", 50), collapse = ""),
      "\n"
    )
  )
  
  library(mongolite)
  
  if (arg == "order") {
    id <- superbidOrder(orderDate = Sys.Date() + 1, id = NULL)                                                           
    cat("Total of orders: ", length(id), "\n")
    
    m <- mongo(collection = "superbidOrder", db = "dw")
    m$insert(jsonlite::toJSON(
      list(
        "_id" = as.integer(Sys.Date()),
        ids = id
      ),
      auto_unbox = TRUE
    ),
    stop_on_error = FALSE)
    m$disconnect()
  }
  
  if (arg == "data") {
    m <- mongo(collection = "superbidOrder", db = "dw")
    ids <-
      m$find(query = paste0('{"_id": ',  as.integer(Sys.Date()), '}'),
             fields = '{"ids": true}')
    ids <- ids$ids[[1]]
    m$disconnect()
    
    if (!is.null(ids)) {
      data <- superbidData(ids)
      cat(
        "\n",
        "Number of ids: ",
        length(ids),
        "\n",
        "Total collected: ",
        length(data),
        "\n",
        "%: ",
        (length(data) / length(ids)) %>% scales::percent(),
        "\n\n"
      )
      
      data <- lapply(data, function(x) {
        names(x)[1] <- "_id"
        return(x)
      })
      
      m <- mongo(collection = "superbidData", db = "dw")
      out <- lapply(data, function(x) {
        m$insert(jsonlite::toJSON(x, auto_unbox = TRUE, pretty = TRUE),
                 stop_on_error = FALSE)
      })
      m$disconnect()
    }
  }
  
  sink()
  library(telegram)
  bot <- TGBot$new(token = bot_token('LogBotAWbot'))
  bot$set_default_chat_id(Sys.getenv("chat_id"))
  s <- readLines(con)
  s <- paste0(s, "\n", collapse = "\n")
  bot$sendMessage(s)
} else {
  print("You must use one of the follow args: cron, order or data!")
}
