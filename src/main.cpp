/**
 * Kostra druhe semestralni prace z predmetu KIV/UPP
 * Soubory a hlavicku upravujte dle sveho uvazeni a nutnosti
 */

#include <string>
#include <vector>
#include <iostream>

#include "utils.h"
#include "server.h"

void process(const std::vector<std::string>& URLs, std::string& vystup) {

	// tuhle metodu implementujte
	// v parametru URLs jsou URL adresy, ktere byly odeslany z formulare

	// tohle nahradte vystupem, ktery chcete zobrazit uzivateli (tj. vysledkem zpracovani)
	vystup = "Zadali jste: <ul>";
	for (const auto& url : URLs) {
		vystup += "<li>" + url + "</li>";
	}
	vystup += "</ul>";

}

int main(int argc, char** argv) {

	// inicializace serveru
	CServer svr;
	if (!svr.Init("./data", "0.0.0.0", 8001)) {
		std::cerr << "Nelze inicializovat server!" << std::endl;
		return EXIT_FAILURE;
	}

	// registrace callbacku pro zpracovani odeslanych URL
	svr.RegisterFormCallback(process);

	// spusteni serveru
	return svr.Run() ? EXIT_SUCCESS : EXIT_FAILURE;
}
