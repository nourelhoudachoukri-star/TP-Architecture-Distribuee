package PartieB;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class LamportDistributedMutex {
    // Nombre de processus testés pour le graphe (question 4)
    private static final int[] NB_PROCESSUS_TESTES = {2, 3, 4, 5, 6};
    // Nombre de passages en SC par processus
    static final int NB_PASSAGES = 2;
    // Probabilité qu'un processus tombe en panne après chaque passage en SC
    // (0.0 pour la question 4 : on mesure seulement le coût en messages)
    static final double PROBA_PANNE = 0.0;
    // Compteur du nombre total d'entrées en section critique
    static final AtomicInteger nbEntreesSC = new AtomicInteger(0);
    // Temps d'attente total (ms) avant d'entrer en SC
    static final AtomicLong tempsAttenteTotal = new AtomicLong(0);

    // Canal réseau simulé (messages envoyés avec délai)
    static class CanalReseau {
        private final Random random = new Random();
        // Probabilité qu'un message soit perdu (0.0 = aucune perte, 0.1 = 10 %)
        private static final double PROBA_PERTE = 0.0;
        private final AtomicInteger compteurMessages = new AtomicInteger(0);
        // Un "tuyau" par couple (source -> destination) pour garder l'ordre FIFO
        private final Map<String, ExecutorService> tuyaux = new ConcurrentHashMap<>();

        // Envoi d'un message avec délai simulé
        public void envoyerMessage(Processus src, Processus dest, Message msg) {
            compteurMessages.incrementAndGet(); // Incrémenter le compteur global
            // Simulation de perte de message
            if (random.nextDouble() < PROBA_PERTE) {
                System.out.println("    xxx Message " + msg.type + " de P" + src.id + " vers P" + dest.id + " PERDU");
                return;
            }
            int delai = 300 + random.nextInt(500); // Délai aléatoire entre 300 et 800 ms
            ExecutorService tuyau = tuyaux.computeIfAbsent(src.id + "->" + dest.id,
                    k -> Executors.newSingleThreadExecutor());
            tuyau.submit(() -> {
                try {
                    Thread.sleep(delai); // Simule la latence du réseau
                    dest.recevoirMessage(msg);
                } catch (InterruptedException ignored) {
                }
            });
        }

        public int getNombreMessages() {
            return compteurMessages.get();
        }

        // Arrête les threads du canal à la fin d'une simulation
        public void arreter() {
            for (ExecutorService tuyau : tuyaux.values()) tuyau.shutdownNow();
        }
    }

    // Types de message possibles
    enum TypeMessage {
        REQUEST, REPLY, RELEASE
    }

    // Classe représentant un message Lamport
    static class Message {
        TypeMessage type;
        int horloge;
        int idSource;

        public Message(TypeMessage type, int horloge, int idSource) {
            this.type = type;
            this.horloge = horloge;
            this.idSource = idSource;
        }
    }

    // Classe représentant un processus dans le système
    static class Processus implements Runnable {
        int id;
        int horlogeLamport = 0;
        CanalReseau canal;
        List<Processus> autres;
        volatile boolean enPanne = false;

        // File triée : horloge puis id
        Queue<Message> fileRequetes = new PriorityBlockingQueue<>(10,
                (a, b) -> a.horloge != b.horloge ? a.horloge - b.horloge : a.idSource - b.idSource);
        Set<Integer> reponsesRecues = ConcurrentHashMap.newKeySet();

        public Processus(int id, CanalReseau canal) {
            this.id = id;
            this.canal = canal;
        }

        // Fonction pour mettre un processus en panne
        public void setEnPanne(boolean enPanne) {
            this.enPanne = enPanne;
        }

        // Mise à jour de l'horloge de Lamport
        private synchronized int incrementerHorloge() {
            horlogeLamport++;
            return horlogeLamport;
        }

        private synchronized void majHorloge(int autreHorloge) {
            horlogeLamport = Math.max(horlogeLamport, autreHorloge) + 1;
        }

        // Envoi d'un message à tous les autres processus
        public void diffuserMessage(Message msg) {
            for (Processus p : autres) {
                if (!p.enPanne) { // Si le destinataire n'est pas en panne
                    canal.envoyerMessage(this, p, msg);
                }
            }
        }

        // Réception d'un message
        public synchronized void recevoirMessage(Message msg) {
            if (enPanne) return; // Ignore si le processus est en panne
            majHorloge(msg.horloge);
            switch (msg.type) {
                case REQUEST:
                    fileRequetes.add(msg);
                    // Répond immédiatement avec un message REPLY
                    canal.envoyerMessage(this, trouverProcessus(msg.idSource),
                            new Message(TypeMessage.REPLY, horlogeLamport, id));
                    break;
                case REPLY:
                    reponsesRecues.add(msg.idSource);
                    break;
                case RELEASE:
                    fileRequetes.removeIf(m -> m.idSource == msg.idSource);
                    break;
            }
        }

        // Trouve un processus par son ID
        private Processus trouverProcessus(int id) {
            return autres.stream().filter(p -> p.id == id).findFirst().orElse(null);
        }

        // Retire de la file les demandes des processus en panne
        private void retirerPannes() {
            fileRequetes.removeIf(m -> m.idSource != id && trouverProcessus(m.idSource).enPanne);
        }

        // Tentative d'entrée dans la section critique
        public void demanderSectionCritique() throws InterruptedException {
            if (enPanne) return;
            System.out.println("P" + id + " demande la section critique");
            long debutAttente = System.currentTimeMillis();
            reponsesRecues.clear();
            Message demande = new Message(TypeMessage.REQUEST, incrementerHorloge(), id);
            fileRequetes.add(demande);
            diffuserMessage(demande); // le MEME message est envoyé à tous

            // Attente des réponses de tous les autres (sauf ceux en panne)
            while (reponsesRecues.size() < autres.size() - nbPannes()) {
                if (enPanne) return;
                Thread.sleep(200);
            }
            // Attente d'être premier dans la file
            while (true) {
                if (enPanne) return;
                retirerPannes();
                Message premier = fileRequetes.peek();
                if (premier != null && premier.idSource == id) break;
                Thread.sleep(100);
            }
            long attente = System.currentTimeMillis() - debutAttente;
            tempsAttenteTotal.addAndGet(attente);
            System.out.println("    (P" + id + " a attendu " + attente + " ms)");
            entrerSectionCritique();
            sortirSectionCritique();
        }

        // Compte combien de processus sont en panne
        private long nbPannes() {
            return autres.stream().filter(p -> p.enPanne).count();
        }

        // Entrée en section critique
        private void entrerSectionCritique() throws InterruptedException {
            nbEntreesSC.incrementAndGet();
            System.out.println("==> P" + id + " entre en section critique (horloge=" + horlogeLamport + ")");
            Thread.sleep(1000 + new Random().nextInt(1000));
        }

        // Sortie de section critique
        private void sortirSectionCritique() {
            System.out.println("<== P" + id + " quitte la section critique");
            fileRequetes.removeIf(m -> m.idSource == id);
            diffuserMessage(new Message(TypeMessage.RELEASE, incrementerHorloge(), id));
            // Affichage de la file courante
            afficherFile();
        }

        private void afficherFile() {
            List<Message> copie = new ArrayList<>(fileRequetes);
            copie.sort((a, b) -> a.horloge != b.horloge ? a.horloge - b.horloge : a.idSource - b.idSource);
            System.out.print("    File de P" + id + " : [ ");
            for (Message m : copie) System.out.print("P" + m.idSource + "(t=" + m.horloge + ") ");
            System.out.println("]");
        }

        @Override
        public void run() {
            try {
                Random random = new Random();
                for (int i = 0; i < NB_PASSAGES && !enPanne; i++) {
                    demanderSectionCritique();
                    // Panne aléatoire selon PROBA_PANNE
                    if (random.nextDouble() < PROBA_PANNE) {
                        System.out.println("!!! P" + id + " tombe en panne !!!");
                        setEnPanne(true);
                        break;
                    }
                    Thread.sleep(500);
                }
            } catch (InterruptedException ignored) {
            }
        }
    }

    // Lance une simulation avec n processus et renvoie le nombre de messages échangés
    static int simuler(int n) throws InterruptedException {
        nbEntreesSC.set(0);
        tempsAttenteTotal.set(0);
        CanalReseau canal = new CanalReseau();

        // Création des processus
        List<Processus> tous = new ArrayList<>();
        for (int i = 1; i <= n; i++) tous.add(new Processus(i, canal));

        // Configuration du réseau (chaque processus connaît les AUTRES, sans lui-même)
        for (Processus p : tous) {
            p.autres = new ArrayList<>(tous);
            p.autres.remove(p);
        }

        // Démarrage des threads et attente de la fin
        List<Thread> threads = new ArrayList<>();
        for (Processus p : tous) {
            Thread t = new Thread(p);
            threads.add(t);
            t.start();
        }
        for (Thread t : threads) t.join();
        Thread.sleep(1000); // laisser arriver les derniers messages
        canal.arreter();

        System.out.println("\n===== STATISTIQUES (N = " + n + ") =====");
        System.out.println("Nombre total de messages échangés : " + canal.getNombreMessages());
        System.out.println("Nombre d'entrées en SC : " + nbEntreesSC.get());
        if (nbEntreesSC.get() > 0)
            System.out.println("Temps moyen d'attente pour entrer en SC : "
                    + (tempsAttenteTotal.get() / nbEntreesSC.get()) + " ms");
        System.out.println("=========================\n");
        return canal.getNombreMessages();
    }

    // ---------- MAIN ----------
    public static void main(String[] args) throws InterruptedException {
        int[] resultats = new int[NB_PROCESSUS_TESTES.length];
        for (int i = 0; i < NB_PROCESSUS_TESTES.length; i++) {
            System.out.println("########## Simulation avec " + NB_PROCESSUS_TESTES[i] + " processus ##########");
            resultats[i] = simuler(NB_PROCESSUS_TESTES[i]);
        }

        // Tableau récapitulatif + graphe en barres dans la console
        System.out.println("===== MESSAGES SELON LE NOMBRE DE PROCESSUS =====");
        System.out.println("N  | Messages | Théorie 3(N-1) x N x " + NB_PASSAGES + " | Graphe");
        for (int i = 0; i < NB_PROCESSUS_TESTES.length; i++) {
            int n = NB_PROCESSUS_TESTES[i];
            int theorie = 3 * (n - 1) * n * NB_PASSAGES;
            String barre = "#".repeat(resultats[i] / 4);
            System.out.printf("%-2d | %-8d | %-26d | %s%n", n, resultats[i], theorie, barre);
        }
        System.exit(0); // arrêter tous les threads
    }
}
