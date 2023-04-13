package com.example.kotlin_spring_integration_test

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.integration.dsl.MessageChannels
import org.springframework.integration.dsl.integrationFlow
import org.springframework.integration.file.dsl.Files
import java.io.File
import java.util.*

@SpringBootApplication
class KotlinSpringIntegrationTestApplication
fun main(args: Array<String>) { runApplication<KotlinSpringIntegrationTestApplication>(*args) }

@Configuration
class ChannelsConfiguration {

    // Define three message channels
    @Bean fun txt() = MessageChannels.direct().get()
    @Bean fun csv() = MessageChannels.direct().get()
    @Bean fun errors() = MessageChannels.direct().get()
}

@Configuration
class FileConfiguration(private val channels: ChannelsConfiguration) {

    // Define the input and output directories
    private val input = File("${System.getenv("HOME")}/Desktop/in")
    private val output = File("${System.getenv("HOME")}/Desktop/out")
    private val csv = File(output, "csv")
    private val txt = File(output, "txt")

    // Create an integration flow to handle errors
    @Bean fun errorFlow() = integrationFlow(channels.errors())
        { handle { println("Error: ${it.payload}") } }

    // Create an integration flow to read files from the input directory
    // and route them to the appropriate channel based on their extension
    @Bean fun filesFlow() = integrationFlow(

        // Define the inbound adapter for reading files
        Files.inboundAdapter(this.input).autoCreateDirectory(true),
        // Configure the polling interval and maximum number
        // of files to read per poll
        { poller { it.fixedDelay(500).maxMessagesPerPoll(1) } }

    ) {
        // Filter out directories
        filter<File> { it.isFile }
        // Route the files to the appropriate channel based on their extension
        route<File> {
            when (it.extension.lowercase(Locale.getDefault())) {
                "csv" -> channels.csv()
                "txt" -> channels.txt()
                else -> channels.errors()
            }
        }
    }

    // Create an integration flow to write CSV files to the output directory
    @Bean fun csvFlow() = integrationFlow(channels.csv())
        { handle(Files.outboundAdapter(csv).autoCreateDirectory(true)) }

    // Create an integration flow to write TXT files to the output directory
    @Bean fun txtFlow() = integrationFlow(channels.txt())
        { handle(Files.outboundAdapter(txt).autoCreateDirectory(true)) }
}