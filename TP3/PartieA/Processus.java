package PartieA;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Processus extends Thread {
    private static int termines = 0;
    private int id;
    private int clock = 0;
    private List<Processus> autres;
    private Queue<Message> fileMessages = new ConcurrentLinkedQueue<>();
    private boolean enSC = false;
    private Set<Integer> reponsesRecues = new HashSet<>();
    private List<Message> fileDemandes = new ArrayList<>();

    public Processus(int id) {
        this.id = id;
    }

    public void setAutres(List<Processus> autres) {
        this.autres = autres;
    }

    // Incrément de l'horloge logique
    private void tick() {
        clock++;
    }

    // Mise à jour horloge sur réception message
    private void majHorloge(int autreHorloge) {
        clock = Math.max(clock, autreHorloge) + 1;
    }

    // Simulation d'envoi de message
    private void envoyer(Message m, Processus dest) {
        dest.recoitMessage(m);
    }

    // Réception d'un message
    public void recoitMessage(Message m) {
        fileMessages.add(m);
    }

    // Diffusion à tous les autres
    private void broadcast(Message m) {
        for (Processus p : autres) {
            if (p.id != this.id)
                envoyer(m, p);
        }
    }

    // Tri de la file : horloge puis id
    private void trierFile() {
        fileDemandes.sort((a, b) -> {
            if (a.timestamp != b.timestamp) return a.timestamp - b.timestamp;
            return a.senderId - b.senderId;
        });
    }

    // Demande d'accès à la section critique
    private void demandeSC() throws InterruptedException {
        System.out.println("Processus " + id + " demande SC à t=" + clock);
        tick();
        Message demande = new Message(Message.Type.REQUEST, id, clock);
        fileDemandes.add(demande);
        trierFile();
        broadcast(demande);
        reponsesRecues.clear();
        reponsesRecues.add(id); // s'auto-approuve

        // Attente : toutes les réponses + ma demande en tête de file
        while (reponsesRecues.size() < autres.size() || fileDemandes.get(0).senderId != id) {
            traiterMessages();
            Thread.sleep(100);
        }
        entrerSC();
    }

    private void entrerSC() throws InterruptedException {
        enSC = true;
        System.out.println("==> Processus " + id + " entre en SC (t=" + clock + ")");
        Thread.sleep(1000); // simulation traitement
        sortirSC();
    }

    private void sortirSC() {
        enSC = false;
        System.out.println("<== Processus " + id + " quitte SC (t=" + clock + ")");
        fileDemandes.removeIf(m -> m.senderId == id);
        tick();
        broadcast(new Message(Message.Type.RELEASE, id, clock));
    }

    private void traiterMessages() {
        while (!fileMessages.isEmpty()) {
            Message m = fileMessages.poll();
            majHorloge(m.timestamp);
            switch (m.type) {
                case REQUEST:
                    fileDemandes.add(m);
                    trierFile();
                    envoyer(new Message(Message.Type.REPLY, id, clock), getProcessus(m.senderId));
                    break;
                case REPLY:
                    reponsesRecues.add(m.senderId);
                    break;
                case RELEASE:
                    fileDemandes.removeIf(x -> x.senderId == m.senderId);
                    break;
            }
        }
    }

    private Processus getProcessus(int id) {
        for (Processus p : autres) {
            if (p.id == id) return p;
        }
        return null;
    }

    private static synchronized void finir() {
        termines++;
    }

    private static synchronized int getTermines() {
        return termines;
    }

    @Override
    public void run() {
        try {
            for (int i = 0; i < 3; i++) {
                Thread.sleep(500 + new Random().nextInt(1000));
                demandeSC();
            }
            // continuer à répondre aux autres jusqu'à la fin
            finir();
            while (getTermines() < autres.size()) {
                traiterMessages();
                Thread.sleep(100);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}