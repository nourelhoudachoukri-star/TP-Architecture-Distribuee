package PartieA;

import java.util.*;

public class LamportExclusion {
    public static void main(String[] args) {
        int N = 3;
        List<Processus> liste = new ArrayList<>();
        for (int i = 0; i < N; i++) liste.add(new Processus(i));
        for (Processus p : liste)
            p.setAutres(liste);
        for (Processus p : liste)
            p.start();
    }
}