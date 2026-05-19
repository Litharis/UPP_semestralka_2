/**
 * Upravená kostra druhé semestrální práce z předmětu KIV/UPP
 * Inicializace MPI a rozdělení rolí (Master, Worker A, Worker B)
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

// Seznam typu zprav pro MPI komunikaci
enum MPITags {
    TAG_URL_TO_WORKER_A = 1, // Master posílá počáteční URL Workeru A
    TAG_URL_TO_WORKER_B = 2, // Worker A posílá URL k parsování Workeru B
    TAG_RESULT_FROM_B = 3,   // Worker B vrací naparsovaná data Workeru A
    TAG_DONE_FROM_A = 4,     // Worker A hlásí Masterovi, že prohledal doménu
    TAG_TERMINATE = 5        // Signál pro ukončení
};

// Pomocná funkce Mastera, která se zavolá, když uživatel odešle formulář na webu
void processMasterDistribution(const std::vector<std::string>& URLs, std::string& vystup, int n, int m) {
	std::cout << "[Master] Prijato " << URLs.size() << " URL adres. Rozdeluji praci..." << std::endl;

	// Odeslání práce Workerům A
	int target_worker_a = 1;
	for (const auto& url : URLs) {
		MPI_Send(url.c_str(), url.size() + 1, MPI_CHAR, target_worker_a, TAG_URL_TO_WORKER_A, MPI_COMM_WORLD);
		target_worker_a++;
		if (target_worker_a > n) target_worker_a = 1;
	}

	// Příprava výpisu pro uživatele
	vystup = "<h3>Zpracování dokončeno!</h3><ul>";

	// Čekání na výsledky od Workerů A
	int completed_jobs = 0;
	while (completed_jobs < URLs.size()) {
		MPI_Status status;
		MPI_Probe(MPI_ANY_SOURCE, TAG_DONE_FROM_A, MPI_COMM_WORLD, &status);
		
		int msg_size;
		MPI_Get_count(&status, MPI_CHAR, &msg_size);
		std::vector<char> buffer(msg_size);
		MPI_Recv(buffer.data(), msg_size, MPI_CHAR, status.MPI_SOURCE, TAG_DONE_FROM_A, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

		std::string payload(buffer.data());
		
		// Rozkouskování přijatých dat (oddělovač je |||)
		size_t pos1 = payload.find("|||");
		size_t pos2 = payload.find("|||", pos1 + 3);
		
		std::string base_url = payload.substr(0, pos1);
		std::string map_txt = payload.substr(pos1 + 3, pos2 - pos1 - 3);
		std::string content_txt = payload.substr(pos2 + 3);

		// Vytvoření bezpečného názvu složky
		auto t = std::time(nullptr);
		auto tm = *std::localtime(&t);
		std::ostringstream time_ss;
		time_ss << std::put_time(&tm, "%Y_%m_%d_%H_%M_");
		
		std::string safe_url = base_url;
		for (char& c : safe_url) {
			if (!isalnum(c)) c = '_'; // Nahradí speciální znaky podtržítkem
		}
		
		std::string dir_name = "results/" + time_ss.str() + safe_url;
		std::filesystem::create_directories(dir_name);

		// Uložení do souborů
		std::ofstream map_file(dir_name + "/map.txt");
		map_file << map_txt;
		
		std::ofstream content_file(dir_name + "/content.txt");
		content_file << content_txt;
		
		std::ofstream log_file(dir_name + "/log.txt");
		log_file << std::put_time(&tm, "%Y-%m-%d %H:%M:%S") << "\nOK\n";

		std::cout << "[Master] Vysledky pro " << base_url << " ulozeny do slozky: " << dir_name << std::endl;
		vystup += "<li>Data pro <b>" + base_url + "</b> uložena do: <code>" + dir_name + "</code></li>";

		completed_jobs++;
	}
	vystup += "</ul><p>Všechny zadané domény byly úspěšně prohledány.</p>";
}

// Funkce, kterou bude vykonávat výhradně uzel Master (Rank 0)
void runMaster(int argc, char** argv, int n, int m) {
	std::cout << "[Master (Rank 0)] Spoustim webovy server..." << std::endl;

	CServer svr;
	if (!svr.Init("./data", "0.0.0.0", 8001)) {
		std::cerr << "Nelze inicializovat server!" << std::endl;
		return;
	}

	// Registrace callbacku pomocí C++ lambdy, abychom do funkce processMasterDistribution
	// dokázali předat parametry n a m (počty workerů)
	svr.RegisterFormCallback([n, m](const std::vector<std::string>& URLs, std::string& vystup) {
		processMasterDistribution(URLs, vystup, n, m);
	});

	// Spuštění serveru. Zde Master uzel zůstane zablokovaný a poslouchá na portu 8001
	svr.Run();
}

// Funkce, kterou budou vykonávat uzly Worker A (Rank 1 až N)
void runWorkerA(int rank, int n, int m) {
	std::cout << "[Worker A (Rank " << rank << ")] Inicializovan a ceka na praci od Mastera..." << std::endl;

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

			// Proměnné pro uložení výsledků celé domény
			std::vector<std::string> map_nodes;
			std::vector<std::pair<std::string, std::string>> map_edges;
			std::string content_data = "";

			while (!url_queue.empty() || active_workers > 0) {
				while (!url_queue.empty() && !free_workers.empty()) {
					std::string next_url = url_queue.front();
					url_queue.pop();
					int worker_b = free_workers.front();
					free_workers.pop();
					MPI_Send(next_url.c_str(), next_url.size() + 1, MPI_CHAR, worker_b, TAG_URL_TO_WORKER_B, MPI_COMM_WORLD);
					active_workers++;
				}

				if (active_workers > 0) {
					MPI_Status result_status;
					MPI_Probe(MPI_ANY_SOURCE, TAG_RESULT_FROM_B, MPI_COMM_WORLD, &result_status);
					
					int res_size;
					MPI_Get_count(&result_status, MPI_CHAR, &res_size);
					std::vector<char> res_buffer(res_size);
					MPI_Recv(res_buffer.data(), res_size, MPI_CHAR, result_status.MPI_SOURCE, TAG_RESULT_FROM_B, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

					free_workers.push(result_status.MPI_SOURCE);
					active_workers--;

					std::string result_msg(res_buffer.data());
					std::istringstream ss(result_msg);
					std::string parsed_url, imgs, forms, links_str, headings_str;
					std::getline(ss, parsed_url, '|');
					std::getline(ss, imgs, '|');
					std::getline(ss, forms, '|');
					std::getline(ss, links_str, '|');
					std::getline(ss, headings_str, '|');

					// ZÁPIS DO DAT PRO MASTERA
					map_nodes.push_back(parsed_url);
					content_data += "\"" + parsed_url + "\"\n";
					content_data += "IMAGES " + imgs + "\n";
					content_data += "FORMS " + forms + "\n";

					int num_links = 0;
					std::istringstream links_ss(links_str);
					std::string link;
					while (std::getline(links_ss, link, ',')) {
						if (link.empty() || link[0] == '#') continue;
						num_links++;

						std::string full_link = link;
						if (link.find("http") != 0) {
							std::string clean_base = base_url;
							if (clean_base.back() == '/' && link[0] == '/') clean_base.pop_back();
							else if (clean_base.back() != '/' && link[0] != '/') clean_base += "/";
							full_link = clean_base + link;
						}

						if (full_link.find(base_url) == 0) {
							map_edges.push_back({parsed_url, full_link}); // Uložíme propojení stránek
							if (visited_urls.find(full_link) == visited_urls.end()) {
								visited_urls.insert(full_link);
								url_queue.push(full_link);
							}
						}
					}
					// Doplníme počet odkazů a nadpisy
					content_data += "LINKS " + std::to_string(num_links) + "\n";
					std::istringstream head_ss(headings_str);
					std::string head;
					while (std::getline(head_ss, head, ',')) {
						content_data += head + "\n";
					}
					content_data += "\n";
				}
			}

			// VŠE HOTOVO: Zabalíme a pošleme Masterovi
			std::string map_data = "";
			for (const auto& node : map_nodes) map_data += "\"" + node + "\"\n";
			for (const auto& edge : map_edges) map_data += "\"" + edge.first + "\" \"" + edge.second + "\"\n";

			// Formát zprávy: BASE_URL ||| MAP_TXT ||| CONTENT_TXT
			std::string final_payload = base_url + "|||" + map_data + "|||" + content_data;
			
			std::cout << "[Worker A (Rank " << rank << ")] Odesilam hotove vysledky Masterovi!" << std::endl;
			MPI_Send(final_payload.c_str(), final_payload.size() + 1, MPI_CHAR, 0, TAG_DONE_FROM_A, MPI_COMM_WORLD);
		}
	}
}

// Funkce, kterou budou vykonávat uzly Worker B (Koncoví dříči, Rank N+1 a vyšší)
void runWorkerB(int rank, int n, int m) {
	int workerA_index = (rank - 1 - n) / m;
	int my_boss_rank = workerA_index + 1;

	std::cout << "[Worker B (Rank " << rank << ")] Ceka na praci od sefa (Rank " << my_boss_rank << ")..." << std::endl;

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

			// === 1. STAŽENÍ STRÁNKY ===
			std::string html = utils::downloadHTML(url);

			if (html.empty()) {
				std::cerr << "[Worker B (Rank " << rank << ")] HTML je prazdne (Chyba stahovani pro " << url << ")." << std::endl;
				// Pošleme šéfovi prázdnou odpověď, aby věděl, že jsme neskončili chybou pádu
				std::string error_msg = url + "|0|0||"; 
				MPI_Send(error_msg.c_str(), error_msg.size() + 1, MPI_CHAR, my_boss_rank, TAG_RESULT_FROM_B, MPI_COMM_WORLD);
				
				running = false; 
				continue;
			}

			// === 2. PARSOVÁNÍ DAT (Obrázky a Formuláře) ===
			int img_count = 0;
			size_t pos = 0;
			while ((pos = html.find("<img ", pos)) != std::string::npos) {
				img_count++;
				pos += 5;
			}

			int form_count = 0;
			pos = 0;
			while ((pos = html.find("<form ", pos)) != std::string::npos) {
				form_count++;
				pos += 6;
			}

			// === 3. PARSOVÁNÍ ODKAZŮ (<a> tagy) ===
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

			// === 4. PARSOVÁNÍ NADPISŮ (<h1> až <h6>) ===
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

			// === 5. ZABALENÍ A ODESLÁNÍ VÝSLEDKŮ ZPĚT ===
			std::string result_msg = url + "|";
			result_msg += std::to_string(img_count) + "|";
			result_msg += std::to_string(form_count) + "|";

			// Sloučení odkazů oddělených čárkou
			for (size_t i = 0; i < links.size(); ++i) {
				result_msg += links[i];
				if (i < links.size() - 1) result_msg += ",";
			}
			result_msg += "|";

			// Sloučení nadpisů oddělených čárkou
			for (size_t i = 0; i < headings.size(); ++i) {
				result_msg += headings[i];
				if (i < headings.size() - 1) result_msg += ",";
			}

			std::cout << "[Worker B (Rank " << rank << ")] Odesilam vysledky zpet sefovi (Worker A Rank " << my_boss_rank << ")..." << std::endl;

			// Odeslání zprávy šéfovi (Worker A) s naším novým štítkem TAG_RESULT_FROM_B
			MPI_Send(result_msg.c_str(), result_msg.size() + 1, MPI_CHAR, my_boss_rank, TAG_RESULT_FROM_B, MPI_COMM_WORLD);
		}
	}
}

int main(int argc, char** argv) {
	// 1. Inicializace MPI prostředí
	// Každý spuštěný proces (ať už Master nebo libovolný Worker) projde touto inicializací
	MPI_Init(&argc, &argv);

	int rank; // Unikátní číslo tohoto konkrétního procesu (0 až size-1)
	int size; // Celkový počet procesů spuštěných přes mpirun
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &size);

	// 2. Parsování parametrů -n a -m z příkazové řádky
	int n = 0; // Počet workerů A
	int m = 0; // Počet workerů B na jednoho workera A

	for (int i = 1; i < argc; ++i) {
		if (std::string(argv[i]) == "-n" && i + 1 < argc) {
			n = std::stoi(argv[i + 1]);
		}
		else if (std::string(argv[i]) == "-m" && i + 1 < argc) {
			m = std::stoi(argv[i + 1]);
		}
	}

	// 3. Validace parametrů a počtu spuštěných MPI procesů
	if (n <= 0 || m <= 0) {
		if (rank == 0) {
			std::cerr << "Chyba: Je nutne zadat parametry -n (pocet workeru A) a -m (pocet workeru B na jednoho A)." << std::endl;
			std::cerr << "Priklad: mpirun -np 10 ./upp2 -n 3 -m 2" << std::endl;
		}
		MPI_Finalize();
		return EXIT_FAILURE;
	}

	// Výpočet očekávaného počtu procesů: 1 (Master) + N (Workerů A) + N*M (Workerů B)
	int expected_size = 1 + n + (n * m);

	if (size != expected_size) {
		if (rank == 0) {
			std::cerr << "Chyba: Pocet spustenych MPI procesu (" << size 
					  << ") neodpoveda vzorci 1 + N + N*M pro zane hodnoty." << std::endl;
			std::cerr << "Pro -n " << n << " a -m " << m << " je ocekavano presne " << expected_size << " procesu!" << std::endl;
		}
		MPI_Finalize();
		return EXIT_FAILURE;
	}

	// 4. Rozcestník: Rozdělení procesů do rolí podle jejich unikátního Ranku
	if (rank == 0) {
		// Pouze proces s ID 0 spustí webový server a bude fungovat jako Master
		runMaster(argc, argv, n, m);
	}
	else if (rank >= 1 && rank <= n) {
		// Procesy s ID 1 až N budou spravovat crawling konkrétních domén
		runWorkerA(rank, n, m);
	}
	else {
		// Všechny ostatní procesy (nad N) jsou koncoví dělníci stahující stránky
		runWorkerB(rank, n, m);
	}

	// 5. Korektní ukončení MPI prostředí před ukončením programu
	MPI_Finalize();
	return EXIT_SUCCESS;
}