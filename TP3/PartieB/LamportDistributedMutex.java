package PartieB;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class LamportDistributedMutex {
    // Nombre total de processus simulés
    private static final int NB_PROCESSUS = 3;
    // Probabilité qu'un processus tombe en panne après chaque passage en SC
    static final double PROBA_PANNE = 0.2;
    // Compteur du nombre total d'entrées en section critique
    static final AtomicInteger nbEntreesSC = new AtomicInteger(0);

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

        public Processus(int
