/**
 * @file main.cpp
 * @brief Semestrální práce KIV/UPP - Distribuovaný webový prohledávač (crawler) s využitím MPI.
 */

#include <string>
#include <vector>
#include <iostream>
#include <mpi.h>
#include <queue>
#include <set>
#include <sstream>
#include <filesystem>
#include <fstream>
#include <chrono>
#include <iomanip>

#include "utils.h"
#include "server.h"

/**
 * @enum MPITags
 * @brief Tagy pro rozlišení jednotlivých MPI zpráv.
 */
enum MPITags {
    TAG_URL_TO_WORKER_A = 1,
    TAG_URL_TO_WORKER_B = 2,
    TAG_RESULT_FROM_B = 3,
    TAG_DONE_FROM_A = 4,
    TAG_TERMINATE = 5
};

/**
 * @brief Rozdá počáteční URL adresy mezi Workery A, počká na výsledky a poskládá z nich výsledné HTML.
 * @param URLs Vektor adres zadaných do formuláře
 * @param vystup Reference na string, do kterého se zapisuje HTML pro prohlížeč
 * @param n Počet procesů Worker A
 * @param m Počet procesů Worker B pod každým Workerem A
 */
void processMasterDistribution(const std::vector<std::string>& URLs, std::string& vystup, int n, int m) {
    // ulozime si cas prijeti pozadavku pro log.txt
    auto t_start = std::time(nullptr);
    auto tm_start = *std::localtime(&t_start);
    std::ostringstream start_time_ss;
    start_time_ss << std::put_time(&tm_start, "%Y-%m-%d %H:%M:%S");
    std::string start_time_str = start_time_ss.str();

    // rozdame URL rovnomerne uzlum Worker A (round-robin)
    int target_worker_a = 1;
    for (const auto& url : URLs) {
        MPI_Send(url.c_str(), url.size() + 1, MPI_CHAR, target_worker_a, TAG_URL_TO_WORKER_A, MPI_COMM_WORLD);
        target_worker_a++;
        if (target_worker_a > n) target_worker_a = 1;
    }

    vystup = "<h3>Zpracování dokončeno!</h3><ul>";

    // pockame, az se zpracuji vsechny zadane domeny 
    int completed_jobs = 0;
    while (completed_jobs < URLs.size()) {
        MPI_Status status;
        MPI_Probe(MPI_ANY_SOURCE, TAG_DONE_FROM_A, MPI_COMM_WORLD, &status);
        
        int msg_size;
        MPI_Get_count(&status, MPI_CHAR, &msg_size);
        std::vector<char> buffer(msg_size);
        MPI_Recv(buffer.data(), msg_size, MPI_CHAR, status.MPI_SOURCE, TAG_DONE_FROM_A, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        std::string payload(buffer.data());
        
        // rozsekame zpravu od workera
        size_t pos1 = payload.find("|||");
        size_t pos2 = payload.find("|||", pos1 + 3);
        
        std::string base_url = payload.substr(0, pos1);
        std::string map_txt = payload.substr(pos1 + 3, pos2 - pos1 - 3);
        std::string content_txt = payload.substr(pos2 + 3);

        // vezmeme aktualni cas pro nazev slozky (lepe zarucime unikatnost nazvu)
        auto t = std::time(nullptr);
        auto tm = *std::localtime(&t);
        std::ostringstream time_ss;
        time_ss << std::put_time(&tm, "%Y_%m_%d_%H_%M_");
        
        // v URL nahradime specialni znaky (lomitka, dvojtecky) podtrzitkem, jinak by padlo vytvareni slozky
        std::string safe_url = base_url;
        for (char& c : safe_url) {
            if (!isalnum(c)) c = '_'; 
        }
        
        std::string dir_name = "results/" + time_ss.str() + safe_url;
        std::filesystem::create_directories(dir_name);

        // ulozime vysledky do textovych souboru
        std::ofstream map_file(dir_name + "/map.txt");
        map_file << map_txt;
        
        std::ofstream content_file(dir_name + "/content.txt");
        content_file << content_txt;
        
        std::ofstream log_file(dir_name + "/log.txt");
        // zapiseme cas staru a konce do log
        log_file << start_time_str << "\n" << std::put_time(&tm, "%Y-%m-%d %H:%M:%S") << "\nOK\n";

        vystup += "<li>Data pro <b>" + base_url + "</b> uložena do: <code>" + dir_name + "</code></li>";

        completed_jobs++;
    }
    vystup += "</ul><p>Všechny zadané domény byly úspěšně prohledány.</p>";
}

/**
 * @brief Řídící proces s rankem 0. Nastartuje HTTP server a po odeslání formuláře zavolá zpracování.
 */
void runMaster(int argc, char** argv, int n, int m) {
    CServer svr;
    if (!svr.Init("./data", "0.0.0.0", 8001)) {
        std::cerr << "[Master] Chyba: Nelze inicializovat server." << std::endl;
        return;
    }

    // napojeni callbacku pro zpracovani odeslaneho formulare
    svr.RegisterFormCallback([n, m](const std::vector<std::string>& URLs, std::string& vystup) {
        processMasterDistribution(URLs, vystup, n, m);
    });

    svr.Run();
}

/**
 * @brief Řídí prohledávání jedné zadané domény.
 * Drží si frontu odkazů, rozdává práci (URL adresy) Workerům B a z toho, co mu vrátí, 
 * skládá mapu stránek. Výsledek potom pošle zpátky Masterovi.
 */
void runWorkerA(int rank, int n, int m) {
    // spocitame si, od jakeho ranku zacinaji nasi podrizeni Workeri B a vlozime je do fronty volnych
    int first_worker_b = 1 + n + (rank - 1) * m;
    std::queue<int> free_workers;
    for (int i = 0; i < m; ++i) {
        free_workers.push(first_worker_b + i);
    }

    bool running = true;
    while (running) {
        MPI_Status status;
        MPI_Probe(0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

        if (status.MPI_TAG == TAG_URL_TO_WORKER_A) {
            int msg_size;
            MPI_Get_count(&status, MPI_CHAR, &msg_size);
            std::vector<char> buffer(msg_size);
            MPI_Recv(buffer.data(), msg_size, MPI_CHAR, 0, TAG_URL_TO_WORKER_A, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            
            std::string base_url(buffer.data());

            std::queue<std::string> url_queue;
            std::set<std::string> visited_urls;
            int active_workers = 0;

            url_queue.push(base_url);
            visited_urls.insert(base_url);

            std::vector<std::string> map_nodes;
            std::vector<std::pair<std::string, std::string>> map_edges;
            std::string content_data = "";

            // hlavni smycka - dokud mame co prochazet nebo nektery Worker B stale pracuje
            while (!url_queue.empty() || active_workers > 0) {
                
                // dokud mame URL ve fronte a mame volne Workery B, pridelime jim praci
                while (!url_queue.empty() && !free_workers.empty()) {
                    std::string next_url = url_queue.front();
                    url_queue.pop();
                    int worker_b = free_workers.front();
                    free_workers.pop();
                    MPI_Send(next_url.c_str(), next_url.size() + 1, MPI_CHAR, worker_b, TAG_URL_TO_WORKER_B, MPI_COMM_WORLD);
                    active_workers++;
                }

                // pokud nekdo pracuje, cekame na vysledek
                if (active_workers > 0) {
                    MPI_Status result_status;
                    MPI_Probe(MPI_ANY_SOURCE, TAG_RESULT_FROM_B, MPI_COMM_WORLD, &result_status);
                    
                    int res_size;
                    MPI_Get_count(&result_status, MPI_CHAR, &res_size);
                    std::vector<char> res_buffer(res_size);
                    MPI_Recv(res_buffer.data(), res_size, MPI_CHAR, result_status.MPI_SOURCE, TAG_RESULT_FROM_B, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                    // worker dopracoval, muzeme ho vratit zpet do fronty volnych
                    free_workers.push(result_status.MPI_SOURCE);
                    active_workers--;

                    // rozparsovani dat, ktera nam worker vratil (oddelovac je |)
                    std::string result_msg(res_buffer.data());
                    std::istringstream ss(result_msg);
                    std::string parsed_url, imgs, forms, links_str, headings_str;
                    std::getline(ss, parsed_url, '|');
                    std::getline(ss, imgs, '|');
                    std::getline(ss, forms, '|');
                    std::getline(ss, links_str, '|');
                    std::getline(ss, headings_str, '|');

                    map_nodes.push_back(parsed_url);
                    content_data += "\"" + parsed_url + "\"\n";
                    content_data += "IMAGES " + imgs + "\n";

                    int num_links = 0;
                    std::istringstream links_ss(links_str);
                    std::string link;
                    
                    // projdeme nalezene odkazy a ulozime si je
                    while (std::getline(links_ss, link, ',')) {
                        if (link.empty() || link[0] == '#') continue;
                        num_links++;

                        std::string full_link = link;
                        
                        // prevod relativniho odkazu na absolutni
                        if (link.find("http") != 0) {
                            std::string clean_base = base_url;
                            if (clean_base.back() == '/' && link[0] == '/') clean_base.pop_back();
                            else if (clean_base.back() != '/' && link[0] != '/') clean_base += "/";
                            full_link = clean_base + link;
                        }

                        // kontrola, jestli odkaz nevede mimo prohledavanou domenu
                        if (full_link.find(base_url) == 0) {
                            map_edges.push_back({parsed_url, full_link});
                            if (visited_urls.find(full_link) == visited_urls.end()) {
                                visited_urls.insert(full_link);
                                url_queue.push(full_link);
                            }
                        }
                    }
                    
                    content_data += "LINKS " + std::to_string(num_links) + "\n";
                    content_data += "FORMS " + forms + "\n";
                    
                    std::istringstream head_ss(headings_str);
                    std::string head;
                    while (std::getline(head_ss, head, ',')) {
                        content_data += head + "\n";
                    }
                    content_data += "\n";
                }
            }

            // slozime vysledek do jednoho stringu pro odeslani masterovi
            std::string map_data = "";
            for (const auto& node : map_nodes) map_data += "\"" + node + "\"\n";
            for (const auto& edge : map_edges) map_data += "\"" + edge.first + "\" \"" + edge.second + "\"\n";

            std::string final_payload = base_url + "|||" + map_data + "|||" + content_data;
            
            MPI_Send(final_payload.c_str(), final_payload.size() + 1, MPI_CHAR, 0, TAG_DONE_FROM_A, MPI_COMM_WORLD);
        }
    }
}

/**
 * @brief Proces Worker B, který zpracovává jednotlivé webové stránky. 
 * Přijme od Workera A konkrétní URL, stáhne HTML kód, spočítá obrázky a formuláře 
 * a vyparsuje odkazy a nadpisy. Získaná data spojí do jednoho řetězce a pošle zpět.
 */
void runWorkerB(int rank, int n, int m) {
    // spocitame si rank naseho nadrazeneho Workera A
    int workerA_index = (rank - 1 - n) / m;
    int my_boss_rank = workerA_index + 1;

    bool running = true;
    while (running) {
        MPI_Status status;
        MPI_Probe(my_boss_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

        if (status.MPI_TAG == TAG_URL_TO_WORKER_B) {
            int msg_size;
            MPI_Get_count(&status, MPI_CHAR, &msg_size);
            std::vector<char> buffer(msg_size);
            MPI_Recv(buffer.data(), msg_size, MPI_CHAR, my_boss_rank, TAG_URL_TO_WORKER_B, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            
            std::string url(buffer.data());

            // stahneme HTML kod stranky
            std::string html = utils::downloadHTML(url);

            if (html.empty()) {
                // pokud se stranku nepovedlo stahnout, vratime prazdnou zpravu, at na nas Worker A neceka
                std::string error_msg = url + "|0|0||"; 
                MPI_Send(error_msg.c_str(), error_msg.size() + 1, MPI_CHAR, my_boss_rank, TAG_RESULT_FROM_B, MPI_COMM_WORLD);
                continue;
            }

            // spocitame obrazky (hledame tag img)
            int img_count = 0;
            size_t pos = 0;
            while ((pos = html.find("<img ", pos)) != std::string::npos) {
                img_count++;
                pos += 5;
            }

            // spocitame formulare
            int form_count = 0;
            pos = 0;
            while ((pos = html.find("<form ", pos)) != std::string::npos) {
                form_count++;
                pos += 6;
            }

            // najdeme vsechny <a> tagy a vytahneme z nich cilovy href
            std::vector<std::string> links;
            pos = 0;
            while ((pos = html.find("<a ", pos)) != std::string::npos) {
                size_t end_tag = html.find(">", pos);
                if (end_tag == std::string::npos) break;

                size_t href_pos = html.find("href=\"", pos);
                if (href_pos != std::string::npos && href_pos < end_tag) {
                    size_t start_quote = href_pos + 6;
                    size_t end_quote = html.find("\"", start_quote);
                    if (end_quote != std::string::npos && end_quote < end_tag) {
                        std::string link = html.substr(start_quote, end_quote - start_quote);
                        links.push_back(link);
                    }
                }
                pos = end_tag;
            }

            // najdeme nadpisy h1 az h6 a pridame k nim pomlcky podle urovne
            std::vector<std::string> headings;
            pos = 0;
            while ((pos = html.find("<h", pos)) != std::string::npos) {
                char level_char = html[pos + 2];
                if (level_char >= '1' && level_char <= '6') {
                    int level = level_char - '0';
                    size_t end_tag = html.find(">", pos);
                    if (end_tag != std::string::npos) {
                        std::string close_tag = "</h"; close_tag += level_char; close_tag += ">";
                        size_t close_pos = html.find(close_tag, end_tag);
                        if (close_pos != std::string::npos) {
                            std::string text = html.substr(end_tag + 1, close_pos - end_tag - 1);
                            std::string dashes(level, '-');
                            headings.push_back(dashes + " " + text);
                            pos = close_pos;
                            continue;
                        }
                    }
                }
                pos += 2;
            }

            // slozime to dohromady (oddeleno svislitkem) a posleme zpet uzlu Worker A
            std::string result_msg = url + "|";
            result_msg += std::to_string(img_count) + "|";
            result_msg += std::to_string(form_count) + "|";

            for (size_t i = 0; i < links.size(); ++i) {
                result_msg += links[i];
                if (i < links.size() - 1) result_msg += ",";
            }
            result_msg += "|";

            for (size_t i = 0; i < headings.size(); ++i) {
                result_msg += headings[i];
                if (i < headings.size() - 1) result_msg += ",";
            }

            MPI_Send(result_msg.c_str(), result_msg.size() + 1, MPI_CHAR, my_boss_rank, TAG_RESULT_FROM_B, MPI_COMM_WORLD);
        }
    }
}

/**
 * @brief Start programu. Inicializuje MPI, zkontroluje parametry z příkazové řádky (-n, -m)
 * a podle přiděleného ranku spustí příslušnou funkci (Master, Worker A, nebo Worker B).
 */
int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);

    int rank;
    int size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    int n = 0; 
    int m = 0; 

    // zpracovani parametru z prikazove radky
    for (int i = 1; i < argc; ++i) {
        if (std::string(argv[i]) == "-n" && i + 1 < argc) {
            n = std::stoi(argv[i + 1]);
        }
        else if (std::string(argv[i]) == "-m" && i + 1 < argc) {
            m = std::stoi(argv[i + 1]);
        }
    }

    // osetreni chybnych vstupu
    if (n <= 0 || m <= 0) {
        if (rank == 0) {
            std::cerr << "Chyba: Chybne nebo chybejici parametry (-n, -m)." << std::endl;
        }
        MPI_Finalize();
        return EXIT_FAILURE;
    }

    // kontrola, jestli pocet spustenych MPI procesu odpovida zadanym parametrum
    int expected_size = 1 + n + (n * m);
    if (size != expected_size) {
        if (rank == 0) {
            std::cerr << "Chyba: Neshoda v poctu spustenych (" << size << ") a ocekavanych (" << expected_size << ") procesu." << std::endl;
        }
        MPI_Finalize();
        return EXIT_FAILURE;
    }

    // rozdeleni roli podle prideleneho ranku
    if (rank == 0) {
        runMaster(argc, argv, n, m);
    }
    else if (rank >= 1 && rank <= n) {
        runWorkerA(rank, n, m);
    }
    else {
        runWorkerB(rank, n, m);
    }

    MPI_Finalize();
    return EXIT_SUCCESS;
}