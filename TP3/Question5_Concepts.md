# Question 5 : Tableau des concepts clés

| Concept | Ce que ça veut dire | Dans notre TP |
|---|---|---|
| Système distribué | Plusieurs machines reliées par un réseau qui travaillent ensemble | Les 3 processus P1, P2, P3 |
| Processus | Un programme qui tourne de façon indépendante | Chaque processus est un thread Java |
| Pas de mémoire partagée | Un processus ne peut pas lire les variables d'un autre, il doit lui envoyer un message | Chaque processus a sa propre horloge et sa propre file |
| Message | L'information envoyée d'un processus à un autre | Classe `Message` avec le type, l'horloge et l'id |
| Canal de communication | Le chemin par lequel passent les messages | Classe `CanalReseau` |
| Latence | Le temps que met un message pour arriver | Délai aléatoire entre 300 et 800 ms |
| Communication synchrone | On envoie et on attend la réponse avant de continuer | Le processus attend les REPLY avant d'entrer en SC |
| Communication asynchrone | On envoie sans attendre la réponse | Les messages sont livrés en arrière-plan |
| RPC | Appeler une fonction sur une autre machine comme si elle était locale | Vu en cours, pas utilisé ici |
| Broadcast (diffusion) | Envoyer le même message à tout le monde | REQUEST et RELEASE envoyés à tous |
| Horloge de Lamport | Un compteur : +1 à chaque événement, et max(local, reçu) + 1 quand on reçoit un message | Permet de savoir qui a demandé la SC en premier |
| Ordre total | Tous les processus classent les demandes de la même façon (horloge, puis id si égalité) | La file d'attente est triée par (horloge, id) |
| Canal FIFO | Les messages arrivent dans le même ordre qu'ils ont été envoyés | Un canal par couple de processus |
| Section critique (SC) | La partie du code qui utilise la ressource partagée | Simulée par un `sleep` d'environ 1 seconde |
| Exclusion mutuelle | Un seul processus à la fois dans la SC | Algorithme de Lamport avec REQUEST / REPLY / RELEASE |
| File d'attente | La liste des demandes en attente, dans l'ordre | `fileRequetes` |
| Perte de messages | Un message est envoyé mais n'arrive jamais | Question 1 : le système se bloque |
| Interblocage (deadlock) | Des processus s'attendent entre eux pour toujours | Quand un RELEASE ou un REPLY est perdu |
| Panne | Un processus s'arrête et ne répond plus | Question 2 : P2 tombe en panne |
| Tolérance aux pannes | Le système continue à marcher même si un processus tombe | P1 et P3 continuent sans P2 |
| Cohérence | Tous les processus ont la même vision des données | Tout le monde a la même file d'attente |
| Consensus | Tous les processus se mettent d'accord sur une décision (Paxos, Raft) | Accord sur l'ordre de passage en SC |
| Coût en messages | Le nombre de messages nécessaires pour une opération | Question 4 : 3(N-1) messages par entrée en SC |
| Temps d'attente | Le temps entre la demande et l'entrée en SC | Question 3 : il augmente quand il y a plus de processus |
| Scalabilité | Est-ce que le système reste efficace quand on ajoute des machines | Avec Lamport, les messages augmentent vite (en N²) |
